def sales_order_time_filter_query(metric, time_period, organization_id='10', limit=10, order_direction=' ', **kwargs):

    time_params = {}

    # Determine time parameters based on the provided time_period
    if time_period == 'monthly':
        specific_period = kwargs.get('specific_period')
        if specific_period:
            try:
                year, month = specific_period.split('-')
                time_params = {'year': year, 'month': month}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-MM'.")
        else:
            raise ValueError("specific_period is required for monthly time period.")

    elif time_period == 'quarterly':
        specific_period = kwargs.get('specific_period')
        if specific_period:
            try:
                year, quarter = specific_period.split('-')
                time_params = {'year': year, 'quarter': quarter}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-QX'.")
        else:
            raise ValueError("Both 'year' and 'quarter' are required for quarterly time period.")

    elif time_period == 'half_yearly':
        specific_period = kwargs.get('specific_period')
        if specific_period:
            try:
                year, half = specific_period.split('-')
                time_params = {'year': year, 'half_year': half}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-HX'.")
        else:
            raise ValueError("Both 'year' and 'half' are required for half-yearly time period.")

    elif time_period == 'annually':
        specific_period = kwargs.get('specific_period')
        if specific_period:
            try:
                year = specific_period
                time_params = {'year': year}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY'.")
        else:
            raise ValueError("Year is required for annual time period.")

    else:
        raise ValueError(f"Unsupported time_period: {time_period}")

    # Prepare the query by replacing the time_filter placeholder with the correct filter
    query = metrics_map.get(metric).format(
        time_filter=time_filters['sales_orders'].get(time_period),  # Updated table name
        order_direction=order_direction,
        limit=limit,
        organization_id=organization_id,
    )

    # Add time filter parameters like year, month, etc.
    # Execute the query with the provided time_params
    conn = connect_to_database()  # Assuming this is a function to connect to the database
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, {
        'organization_id': organization_id,
        **time_params
    })
    result = cursor.fetchall()
    return result


def purchase_order_time_filter_query(metric, time_period, organization_id = '10', limit=int,order_direction=' ',**kwargs):

   time_params = {}

    # Determine time parameters based on the provided time_period
   if time_period == 'monthly':
        specific_period = kwargs.get('specific_period')
        if specific_period:
            try:
                year, month = specific_period.split('-')
                time_params = {'year': year, 'month': month}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-MM'.")
        else:
            raise ValueError("specific_period is required for monthly time period.")

   elif time_period == 'quarterly':
        specific_period = kwargs.get('specific_period')
        if specific_period:
           try:
              year = specific_period
              quarter = '2'
              time_params = {'year': year, 'quarter': quarter}
           except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-QX'.")
        else:
            raise ValueError("Both 'year' and 'quarter' are required for quarterly time period.")


   elif time_period == 'half_yearly':
         specific_period = kwargs.get('specific_period')
         if specific_period:
            try:
              year = specific_period
              half = '1'
              time_params = {'year': year, 'half_year': half}
            except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY-HX'.")
         else:
            raise ValueError("Both 'year' and 'half' are required for half-yearly time period.")

   elif time_period == 'annually':
        specific_period = kwargs.get('specific_period')
        if specific_period:
          try :
            year = specific_period
            time_params = {'year': year}
          except ValueError:
                raise ValueError(f"Invalid specific_period format: {specific_period}. Expected 'YYYY'.")
        else:
            raise ValueError("Year is required for annual time period.")

   else:
        raise ValueError(f"Unsupported time_period: {time_period}")

    # Prepare the query by replacing the time_filter placeholder with the correct filter
   query = metrics_map.get(metric).format(
        time_filter=time_filters['purchase_orders'].get(time_period),
        order_direction=order_direction,
        limit=limit,
    )
  # Add time filter parameters like year, month, etc.
  # Execute the query with the provided time_params
   conn = connect_to_database()
   cursor = conn.cursor(dictionary=True)
   cursor.execute(query, {
        'organization_id': organization_id,
        **time_params
    })
   result = cursor.fetchall()
   return result


def fetch_item_details(identifier, identifier_type, organization_id='10', time_period=None, specific_period=None, limit=10):
    """
    Fetch item details based on either SKU or item name.

    :param identifier: SKU or item name based on identifier_type
    :param identifier_type: 'sku' or 'name' to determine search criteria
    :param organization_id: Organization ID (default: '10')
    :param time_period: Time filter type (e.g., 'monthly', 'quarterly', 'annually')
    :param specific_period: Specific period string (e.g., '2025-01' for monthly)
    :param limit: Limit number of results (default: 10)
    :return: Item details (sku, name, buying price, selling price)
    """
    time_params = {}
    time_filter = ""

    # Apply time-based filters if provided
    if time_period and specific_period:
        if time_period == 'monthly':
            try:
                year, month = map(int, specific_period.split('-'))
                time_filter = "AND YEAR(s.order_date) = %(year)s AND MONTH(s.order_date) = %(month)s"
                time_params = {'year': year, 'month': month}
            except ValueError:
                raise ValueError("Invalid format for monthly period. Expected 'YYYY-MM'.")
        elif time_period == 'quarterly':
            try:
                year, quarter = map(int, specific_period.split('-Q'))
                time_filter = "AND YEAR(s.order_date) = %(year)s AND QUARTER(s.order_date) = %(quarter)s"
                time_params = {'year': year, 'quarter': quarter}
            except ValueError:
                raise ValueError("Invalid format for quarterly period. Expected 'YYYY-QX'.")
        elif time_period == 'half_yearly':
            try:
                year, half = map(int, specific_period.split('-H'))
                time_filter = "AND YEAR(s.order_date) = %(year)s AND CASE WHEN MONTH(s.order_date) BETWEEN 1 AND 6 THEN 1 ELSE 2 END = %(half_year)s"
                time_params = {'year': year, 'half_year': half}
            except ValueError:
                raise ValueError("Invalid format for half-yearly period. Expected 'YYYY-HX'.")
        elif time_period == 'annually':
            try:
                year = int(specific_period)
                time_filter = "AND YEAR(s.order_date) = %(year)s"
                time_params = {'year': year}
            except ValueError:
                raise ValueError("Invalid format for annual period. Expected 'YYYY'.")
        else:
            raise ValueError("Unsupported time_period provided.")

    # Determine the correct query and parameters based on identifier type
    if identifier_type == 'sku':
        metric = 'item_sku'
        param_key = 'sku'
    elif identifier_type == 'name':
        metric = 'item_name'
        param_key = 'name'
    else:
        raise ValueError("Invalid identifier_type. Use 'sku' or 'name'.")

    # Ensure metric exists in the metrics map
    if metric not in metrics_map:
        raise ValueError(f"Metric '{metric}' not found in metrics_map.")

    if time_period and time_period not in time_filters['sales_orders']:
        raise ValueError(f"Time filter for '{time_period}' not found in time_filters.")

    query = metrics_map[metric].format(
        time_filter=time_filters['sales_orders'].get(time_period, "")
    )

    # Merge parameters
    params = {
        'organization_id': organization_id,
        param_key: identifier,
        'limit': limit,
        **time_params
    }

    conn = connect_to_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, params)
    result = cursor.fetchall()

    # Close connection
    cursor.close()
    conn.close()

    return result


def fetch_customer_details_by_name(contact_name, organization_id='10', time_period=None, specific_period=None, limit=10, order_direction='DESC'):
    """
    Fetch customer details based on customer name with optional time filters.

    :param contact_name: Name of the customer.
    :param organization_id: Organization ID (default: '10')
    :param time_period: Time filter type (e.g., 'monthly', 'quarterly', 'annually')
    :param specific_period: Specific period string (e.g., '2025-01' for monthly)
    :param limit: Limit number of results (default: 10)
    :param order_direction: The direction to order the results, either 'ASC' or 'DESC' (default: 'DESC')
    :return: Customer details (name, contact info, sales data, etc.)
    """
    time_params = {}
    time_filter = ""

    # Apply time-based filters if provided
    if time_period and specific_period:
        if time_period == 'monthly':
            try:
                year, month = map(int, specific_period.split('-'))
                time_filter = "AND YEAR(c.created_at) = %(year)s AND MONTH(c.created_at) = %(month)s"
                time_params = {'year': year, 'month': month}
            except ValueError:
                raise ValueError("Invalid format for monthly period. Expected 'YYYY-MM'.")
        elif time_period == 'quarterly':
            try:
                year, quarter = map(int, specific_period.split('-Q'))
                time_filter = "AND YEAR(c.created_at) = %(year)s AND QUARTER(c.created_at) = %(quarter)s"
                time_params = {'year': year, 'quarter': quarter}
            except ValueError:
                raise ValueError("Invalid format for quarterly period. Expected 'YYYY-QX'.")
        elif time_period == 'half_yearly':
            try:
                year, half = map(int, specific_period.split('-H'))
                time_filter = "AND YEAR(c.created_at) = %(year)s AND CASE WHEN MONTH(c.created_at) BETWEEN 1 AND 6 THEN 1 ELSE 2 END = %(half_year)s"
                time_params = {'year': year, 'half_year': half}
            except ValueError:
                raise ValueError("Invalid format for half-yearly period. Expected 'YYYY-HX'.")
        elif time_period == 'annually':
            try:
                year = int(specific_period)
                time_filter = "AND YEAR(c.created_at) = %(year)s"
                time_params = {'year': year}
            except ValueError:
                raise ValueError("Invalid format for annual period. Expected 'YYYY'.")
        else:
            raise ValueError("Unsupported time_period provided.")

    # Ensure the correct metric (customer by name) is used for the query
    metric = 'customers_by_name'
    if metric not in metrics_map:
        raise ValueError(f"Metric '{metric}' not found in metrics_map.")

    # Retrieve the query from metrics_map
    query = metrics_map[metric]

    # Format the query with the time filter and order direction
    formatted_query = query.format(
        time_filter=time_filter,  # Add the time filter dynamically
        order_direction=order_direction  # Add the order direction
    )

    # Merge parameters for the query execution
    params = {
        'organization_id': organization_id,
        'contact_name': contact_name,  # Customer's name as the identifier
        'limit': limit,
        **time_params  # Add time-based parameters (year, month, etc.)
    }

    # Connect to the database and execute the query
    conn = connect_to_database()  # Assuming this function connects to your database
    cursor = conn.cursor(dictionary=True)
    cursor.execute(formatted_query, params)
    result = cursor.fetchall()

    # Close the connection
    cursor.close()
    conn.close()

    return result
