----------------------------------------
-- cassandra
----------------------------------------
create keyspace invoice with replication ={'class': 'SimpleStrategy', 'replication_factor':1};
use invoice;

CREATE TABLE invoice ( PRIMARY KEY (number), number int, name text, address text,country text, value double);

CREATE TABLE invoice_item (
  PRIMARY KEY (number,invoice_item_id),
  number int,invoice_item_id int,service text,quantity int,unit_value double,source text,qualification text,
  tax_percent double, discount_percent double,subtotal double);