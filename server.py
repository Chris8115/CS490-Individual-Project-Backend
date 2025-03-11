from flask import Flask, jsonify, request
import mysql.connector
from datetime import datetime
from decimal import Decimal

app = Flask(__name__)

# Database connection config
db_config = {
  "host": "localhost",
  "user": "root",
  "password": "8115",
  "database": "sakila"
}

def convert_data(obj):
  if isinstance(obj, datetime):
    return obj.strftime('%Y-%m-%d %H:%M:%S')
  elif isinstance(obj, Decimal):
    return float(obj)
  elif isinstance(obj, set):
    return list(obj)
  return obj

@app.route('/top_rented_films', methods=['GET'])
def get_top_rented_films():
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor(dictionary=True)
  query = """
    SELECT 
      f.film_id, f.title, f.description, f.release_year, f.language_id, 
      f.original_language_id, f.rental_duration, f.rental_rate, f.length, 
      f.replacement_cost, f.rating, f.special_features, f.last_update,
      COUNT(r.rental_id) AS rental_count
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    GROUP BY f.film_id
    ORDER BY rental_count DESC
    LIMIT 5;
  """
  cursor.execute(query)
  films = cursor.fetchall()
  for film in films:
    for key, value in film.items():
      if key == "special_features" and isinstance(value, str):
        film[key] = value.split(',')
      else:
        film[key] = convert_data(value)
  cursor.close()
  conn.close()
  return jsonify(films)

@app.route('/film_inventory/<int:film_id>', methods=['GET'])
def get_film_inventory(film_id):
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor(dictionary=True)
  
  cursor.execute("SELECT COUNT(*) AS total_inventory FROM inventory WHERE film_id = %s", (film_id,))
  total = cursor.fetchone()
  
  available_query = """
    SELECT COUNT(*) AS available_inventory 
    FROM inventory 
    WHERE film_id = %s 
      AND inventory_id NOT IN (
          SELECT inventory_id FROM rental WHERE return_date IS NULL
      )
  """
  cursor.execute(available_query, (film_id,))
  available = cursor.fetchone()
  
  cursor.close()
  conn.close()
  
  return jsonify({
    "total_inventory": total["total_inventory"],
    "available_inventory": available["available_inventory"]
  })

@app.route('/return_film', methods=['POST'])
def return_film():
  data = request.get_json()
  rental_id_input = data.get('rental_id')
  customer_id = data.get('customer_id')
  film_id = data.get('film_id')  # Optional
  
  # If rental_id is not "0" or empty, use it; otherwise, treat as not provided.
  rental_id = rental_id_input if rental_id_input and str(rental_id_input).strip() not in ["", "0"] else None
  
  # If no rental_id and no customer_id provided, return error.
  if rental_id is None and (customer_id is None or str(customer_id).strip() == ""):
    return jsonify({"error": "Missing rental_id or customer_id"}), 400
  
  customer_str = str(customer_id).strip() if customer_id is not None else ""
  
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor()
  
  # If a valid rental_id is provided, update that record.
  if rental_id:
    update_query = """
      UPDATE rental
      SET return_date = NOW()
      WHERE rental_id = %s AND return_date IS NULL
    """
    cursor.execute(update_query, (rental_id,))
    if cursor.rowcount == 0:
      conn.commit()
      cursor.close()
      conn.close()
      return jsonify({"error": "Rental not found or already returned"}), 400
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Film returned successfully", "rental_id": rental_id})
  
  # If customer_id is "0", return all active rentals.
  elif customer_str == "0":
    update_query = """
      UPDATE rental
      SET return_date = NOW()
      WHERE return_date IS NULL
    """
    cursor.execute(update_query)
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()
    return jsonify({
      "message": "All films returned successfully",
      "returned_count": affected
    })
  
  # Otherwise, return the oldest active rental for the given customer (and film if provided)
  else:
    customer_id_val = int(customer_id)
    if film_id:
      film_id_val = int(film_id)
      select_query = """
        SELECT r.rental_id 
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        WHERE r.customer_id = %s AND i.film_id = %s AND r.return_date IS NULL
        ORDER BY r.rental_date ASC
        LIMIT 1
      """
      cursor.execute(select_query, (customer_id_val, film_id_val))
    else:
      select_query = """
        SELECT rental_id 
        FROM rental
        WHERE customer_id = %s AND return_date IS NULL
        ORDER BY rental_date ASC
        LIMIT 1
      """
      cursor.execute(select_query, (customer_id_val,))
    
    result = cursor.fetchone()
    if not result:
      cursor.close()
      conn.close()
      return jsonify({"error": "No active rental found for the provided customer/film"}), 400
    
    rental_id = result[0]
    update_query = """
      UPDATE rental
      SET return_date = NOW()
      WHERE rental_id = %s
    """
    cursor.execute(update_query, (rental_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Film returned successfully", "rental_id": rental_id})

@app.route('/top_actors', methods=['GET'])
def get_top_actors():
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor(dictionary=True)
  query = """
    SELECT 
      a.actor_id, 
      CONCAT(a.first_name, ' ', a.last_name) AS actor_name, 
      COUNT(fa.film_id) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN inventory i ON fa.film_id = i.film_id
    GROUP BY a.actor_id
    ORDER BY film_count DESC
    LIMIT 5;
  """
  cursor.execute(query)
  actors = cursor.fetchall()
  cursor.close()
  conn.close()
  return jsonify(actors)

@app.route('/actor_films/<int:actor_id>', methods=['GET'])
def get_actor_top_films(actor_id):
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor(dictionary=True)
  query = """
    SELECT 
      f.film_id, f.title, COUNT(r.rental_id) AS rental_count
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    JOIN film_actor fa ON f.film_id = fa.film_id
    WHERE fa.actor_id = %s
    GROUP BY f.film_id
    ORDER BY rental_count DESC
    LIMIT 5;
  """
  cursor.execute(query, (actor_id,))
  films = cursor.fetchall()
  cursor.close()
  conn.close()
  return jsonify(films)

@app.route('/search', methods=['GET'])
def search_films():
    search_type = request.args.get('type')
    query_param = request.args.get('query')
    if not search_type or not query_param:
        return jsonify([])
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    # Define the columns to return, qualified with alias "f"
    columns = """
      f.film_id, f.title, f.description, f.release_year,
      f.language_id, f.original_language_id, f.rental_duration,
      f.rental_rate, f.length, f.replacement_cost, f.rating,
      f.special_features, f.last_update
    """
    
    if search_type == "film":
        sql = f"""
        SELECT {columns}
        FROM film f
        WHERE f.title LIKE %s
        """
        cursor.execute(sql, ('%' + query_param + '%', ))
    
    elif search_type == "actor":
        sql = f"""
        SELECT DISTINCT {columns}
        FROM film f
        JOIN film_actor fa ON f.film_id = fa.film_id
        JOIN actor a ON fa.actor_id = a.actor_id
        WHERE CONCAT_WS(' ', a.first_name, a.last_name) LIKE %s
        """
        cursor.execute(sql, ('%' + query_param + '%', ))
    
    elif search_type == "genre":
        sql = f"""
        SELECT DISTINCT {columns}
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE c.name LIKE %s
        """
        cursor.execute(sql, ('%' + query_param + '%', ))
    
    else:
        cursor.close()
        conn.close()
        return jsonify([])
    
    films = cursor.fetchall()
    
    for film in films:
        for key, value in film.items():
            film[key] = convert_data(value)
    
    cursor.close()
    conn.close()
    return jsonify(films)




@app.route('/rent_film', methods=['POST'])
def rent_film():
  data = request.get_json()
  film_id = data.get('film_id')
  customer_id = data.get('customer_id')
  if not film_id or not customer_id:
    return jsonify({"error": "Missing film_id or customer_id"}), 400
  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor()
  query = """
    SELECT inventory_id 
    FROM inventory 
    WHERE film_id = %s 
      AND inventory_id NOT IN (
          SELECT inventory_id FROM rental WHERE return_date IS NULL
      )
    LIMIT 1;
  """
  cursor.execute(query, (film_id,))
  result = cursor.fetchone()
  if not result:
    cursor.close()
    conn.close()
    return jsonify({"error": "Film not available for rent"}), 400
  inventory_id = result[0]
  insert_query = """
    INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
    VALUES (NOW(), %s, %s, 1)
  """
  cursor.execute(insert_query, (inventory_id, customer_id))
  conn.commit()
  rental_id = cursor.lastrowid
  cursor.close()
  conn.close()
  return jsonify({"message": "Film rented successfully", "rental_id": rental_id})

@app.route('/customers', methods=['GET'])
def get_customers():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT customer_id, store_id, first_name, last_name, email, active, create_date
    FROM customer
    ORDER BY customer_id
    """
    
    cursor.execute(query)
    customers = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(customers)



@app.route('/customers/search', methods=['GET'])
def search_customers():
    search_type = request.args.get('type')
    query_param = request.args.get('query')

    if not search_type or not query_param:
        return jsonify([])

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    sql = ""
    if search_type == "customer_id":
        sql = "SELECT customer_id, first_name, last_name, email FROM customer WHERE customer_id = %s"
        cursor.execute(sql, (query_param,))
    elif search_type == "first_name":
        sql = "SELECT customer_id, first_name, last_name, email FROM customer WHERE first_name LIKE %s"
        cursor.execute(sql, ('%' + query_param + '%',))
    elif search_type == "last_name":
        sql = "SELECT customer_id, first_name, last_name, email FROM customer WHERE last_name LIKE %s"
        cursor.execute(sql, ('%' + query_param + '%',))

    customers = cursor.fetchall()
    cursor.close()
    conn.close()

    return jsonify(customers)

@app.route('/customers/add', methods=['POST'])
def add_customer():
    data = request.get_json()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    store_id = data.get('store_id')
    address_id = data.get('address_id')

    if not first_name or not last_name or not email or not store_id or not address_id:
        return jsonify({"error": "Missing required fields"}), 400

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    insert_query = """
    INSERT INTO customer (store_id, first_name, last_name, email, address_id, create_date)
    VALUES (%s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(insert_query, (store_id, first_name, last_name, email, address_id))
    conn.commit()
    new_customer_id = cursor.lastrowid

    cursor.execute("SELECT customer_id, store_id, first_name, last_name, email, address_id FROM customer WHERE customer_id = %s", (new_customer_id,))
    new_customer = cursor.fetchone()

    cursor.close()
    conn.close()

    return jsonify(new_customer)

@app.route('/edit_customer/<int:customer_id>', methods=['PUT'])
def edit_customer(customer_id):
    data = request.get_json()
    
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    update_query = """
    UPDATE customer
    SET store_id = %s, first_name = %s, last_name = %s, email = %s, active = %s
    WHERE customer_id = %s
    """
    
    cursor.execute(update_query, (
        data['store_id'],
        data['first_name'],
        data['last_name'],
        data['email'],
        int(data['active']),
        customer_id
    ))

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"message": "Customer updated successfully"})

@app.route('/delete_customer/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Delete related payments first (to avoid FK errors)
        cursor.execute("DELETE FROM payment WHERE customer_id = %s", (customer_id,))

        # Delete related rentals if cascade isn't enabled
        cursor.execute("DELETE FROM rental WHERE customer_id = %s", (customer_id,))

        # Delete customer after related records are gone
        cursor.execute("DELETE FROM customer WHERE customer_id = %s", (customer_id,))

        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Customer deleted successfully"}), 200

    except mysql.connector.Error as err:
        conn.rollback() 
        cursor.close()
        conn.close()
        print(f"Error deleting customer: {str(err)}") 
        return jsonify({"error": f"Error deleting customer: {str(err)}"}), 500





if __name__ == "__main__":
  app.run(debug=True)
