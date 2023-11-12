schema = {
"customer_id": "string",
"marketplace": "string",
"review_id": "string",
"product_id": "string",
"product_parent": "string",
"product_title": "string",
"product_category": "string",
"star_rating": "int",
"helpful_votes": "int",
"total_votes": "int",
"vine": "string",
"verified_purchase": "string",
"review_headline": "string",
"review_body": "string",
"purchase_date": "date",
"review_timestamp": "timestamp",
"customer_name": "string",
"gender": "string",
"date_of_birth": "string",
"city": "string",
"state": "string"
}

for key, value in schema.items():
    value = value.capitalize()
    print(f'StructField("{key}", {value}Type(), nullable=True),')
