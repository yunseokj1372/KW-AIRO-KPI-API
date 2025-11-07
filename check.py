query = """
SELECT * FROM opticket WHERE ticketno = 1234567890
"""

new_query = """FROM filtered_tickets t
WHERE t.ticketno = 1234567890"""

result = query + new_query

print(result)