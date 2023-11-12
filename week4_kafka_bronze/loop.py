columns = {"marketplace": "string",
"customer_id": "string",
"review_id": "string",
"product_id": "string",
"product_parent": "string",
"product_title": "string",
"product_category": "string",
"star_rating": "int",
"helpful_votes": "int",
"total_votes": "int",
"vine": "string",
"verified_purchase" :"string",
"review_headline": "string",
"review_body": "string",
"purchase_date": "date"}

arr = []
i = 0
for key, value in columns.items():
    arr.append(f"value[{i}] as {key}, {value}")
    i = i +1


# j = 0
# arrnames = []
# for name, valtype in columns.items():
#     if valtype != "string":
#         arrnames.append(f"CAST(value[{j}] AS {valtype}) as {name}, \\ \n ")
#     elif valtype == "string":
#         arrnames.append(f"value[{j}] as {name}, \\ \n ")
#     j = j +1

j = 0
arrnames = []
for name, valtype in columns.items():
    if valtype != "string":
        arrnames.append(f"'CAST(value[{j}] AS {valtype}) as {name}'")
    elif valtype == "string":
        arrnames.append(f"'value[{j}] as {name}'")
    j = j +1

arrnames_string = ', '.join(arrnames).rstrip(', ')

print("arrnames", arrnames_string)

string = '\\'
print('\\')