package atividade6.invoice;

import atividade6.model.Invoice;
import atividade6.model.InvoiceItem;
import com.datastax.driver.core.Row;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    static java.sql.Connection mysqlConn = null;
    static java.sql.PreparedStatement mysqlPrepareStat = null;
    static com.datastax.driver.core.Cluster cluster = null;
    static com.datastax.driver.core.Session session = null;

    public static void main(String args[]) throws UnknownHostException, FileNotFoundException {

        Scanner s = new Scanner(System.in);

        System.out.println("Selecione uma opção:");
        System.out.println("1 - Sincronizar");
        System.out.println("2 - Gerar PDF de NF");
        System.out.println("--------------------");
        String opcao = s.next();

        if (opcao.equals("1")) {
            mysqlJDBCConnection();
            cassandraConnection();
            mysqlToCassandra();
        } else if (opcao.equals("2")) {
            System.out.println("Informe o número da NF:");
            opcao = s.next();
            System.out.println("Você informou: " + opcao);

            //Obtém a nota fiscal do banco de dados
            Main m = new Main();
            int numberInvoice = Integer.parseInt(opcao);
            Invoice nf = m.getNotaCassandra(numberInvoice);

            //Gera o pdf da nota fiscal
            PdfInvoice pdf = new PdfInvoice(nf);
            String caminho = pdf.gerarPdfNotaFiscal();
            System.out.println("Nota fiscal gerada na raiz do projeto: " + caminho);

        }

    }

    private static void mysqlJDBCConnection() {

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            // DriverManager: The basic service for managing a set of JDBC drivers.
            mysqlConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/atividade6", "root", "rootroot");
            if (mysqlConn != null) {
            } else {
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

    }

    public static void cassandraConnection() {
        //com.datastax.driver.core.Cluster cluster = null;

        try {
            cluster = com.datastax.driver.core.Cluster.builder() // (1)
                    .addContactPoint("127.0.0.1")
                    .build();
            //com.datastax.driver.core.Session 
            session = cluster.connect("invoice");                                           // (2)
        } finally {
            //  if (cluster != null) cluster.close();                                          // (5)
        }
    }

    public Invoice getNotaCassandra(int codnot) {

        cassandraConnection();
        com.datastax.driver.core.ResultSet rs = session.execute("select * from invoice where number = " + codnot);

        List<Row> nf = rs.all();
        Invoice notaFiscal = null;

        //Cria a nota somente se houver itens na mesma
        if (nf.size() == 1) {

            notaFiscal = new Invoice();

            //Adiciona os metadados a nota fiscal
            for (int i = 0; i < nf.size(); i++) {
                Row linha = nf.get(i);

                notaFiscal.number = linha.getInt("number");
                notaFiscal.address = linha.getString("address");
                notaFiscal.county = linha.getString("country");
                notaFiscal.name = linha.getString("name");
                notaFiscal.value = linha.getDouble("value");

            }

            //Busca os itens da Nota Fiscal
            List<InvoiceItem> listaItens = new ArrayList();
            com.datastax.driver.core.ResultSet rsItem = session.execute("select * from invoice_item where number = " + codnot);
            List<Row> itens = rsItem.all();

            //Adiciona os itens na nota fiscal
            for (int i = 0; i < itens.size(); i++) {
                Row linhaItem = itens.get(i);

                InvoiceItem item = new InvoiceItem();
                item.number = linhaItem.getInt("number");
                item.invoiceItemId = linhaItem.getInt("invoice_item_id");
                item.discountPercent = linhaItem.getDouble("discount_percent");
                item.qualification = linhaItem.getString("qualification");
                item.quantity = linhaItem.getInt("quantity");
                item.service = linhaItem.getString("service");
                item.source = linhaItem.getString("source");
                item.subtotal = linhaItem.getDouble("subtotal");
                item.taxPercent = linhaItem.getDouble("tax_percent");
                item.unitValue = linhaItem.getDouble("unit_value");

                listaItens.add(item);
            }

            notaFiscal.itens = listaItens;

        }

        session.close();
        cluster.close();

        return notaFiscal;

    }

    private static void mysqlToCassandra() {
        try {
            String getQueryStatement = "select i.number, c.name,c.address,c.country,i.value from invoice i inner join customer c on c.id_customer = i.customer_id";

            mysqlPrepareStat = mysqlConn.prepareStatement(getQueryStatement);

            // Execute the Query, and get a java ResultSet
            java.sql.ResultSet rs = mysqlPrepareStat.executeQuery();
            /*
			+--------+----------+-----------------+---------+-------------------+
			| number | name     | address         | country | value             |
			+--------+----------+-----------------+---------+-------------------+
			|   1960 | Infineon | Am Campeon 1-15 | Germany | 8349.549998939037 |
			+--------+----------+-----------------+---------+-------------------+			
             */
            //Clear invoice table
            session.execute("truncate invoice");
            // Let's iterate through the java ResultSet
            while (rs.next()) {
                int invoice_number = rs.getInt("number");
                String client_name = rs.getString("name");
                String client_address = rs.getString("address");
                String client_country = rs.getString("country");
                double invoice_value = rs.getDouble("value");

                session.execute("insert into invoice (number, name, address,country, value) values ("
                        + invoice_number
                        + ",'" + client_name + "'"
                        + ",'" + client_address + "'"
                        + ",'" + client_country + "'"
                        + "," + invoice_value + ")"
                );
            }
            rs.close();
            mysqlPrepareStat.close();

            /*
			getQueryStatement = "select i.number,d.invoice_item_id,s.service_description,d.quantity,d.unit_value,r.name,q.qualificatin_name,d.tax_percent,d.discount_percent,d.subtotal " + 
					"from resource r " + 
					"inner join resource_qualification_assignement a on a.resource_id = r.id_resource " + 
					"inner join resource_qualification q on q.id_resource_qualification = a.qualification_id " + 
					"inner join invoice_item d on d.resource_id = r.id_resource " + 
					"inner join invoice i on i.number = d.invoice_id " + 
					"inner join service s on s.service_id = d.service_id " + 
					"where a.assignement_date = ( " + 
					"select max(assignement_date)  " + 
					"from resource_qualification_assignement qa  " + 
					"where qa.resource_id = r.id_resource " + 
					"and qa.assignement_date <= i.emission_date " + 
					")";
             */
            getQueryStatement = "select d.invoice_id number, "
                    + " d.invoice_item_id, "
                    + " (select service_description from service where service_id = d.service_id) service_description, "
                    + " d.quantity, "
                    + " d.unit_value, "
                    + " (select name from resource where id_resource = d.resource_id) name, "
                    + " (select qualificatin_name from resource_qualification q "
                    + " where q.id_resource_qualification =  ( "
                    + "   select qualification_id from resource_qualification_assignement where id_qualification_assignement = ( "
                    + "     select max(qa.id_qualification_assignement) "
                    + "     from resource_qualification_assignement qa "
                    + "     where qa.assignement_date <= d.date_of_reference "
                    + "     and qa.resource_id = d.resource_id))) as qualificatin_name, "
                    + " d.tax_percent, "
                    + " d.discount_percent, "
                    + " d.subtotal "
                    + " from invoice_item d order by 1,2";

            mysqlPrepareStat = mysqlConn.prepareStatement(getQueryStatement);

            // Execute the Query, and get a java ResultSet
            rs = mysqlPrepareStat.executeQuery();

            /*
+--------+-----------------+------------------------------------+----------+------------+-----------------+-------------------+-------------+------------------+--------------------+
| number | invoice_item_id | service_description                | quantity | unit_value | name            | qualificatin_name | tax_percent | discount_percent | subtotal           |
+--------+-----------------+------------------------------------+----------+------------+-----------------+-------------------+-------------+------------------+--------------------+
|   1960 |            2325 | PHP Developer for Software House   |       22 |         35 | Jeff Sutherland | Plain             |        0.18 |             0.08 |  847.0000068843365 |
             */
            //Clear invoice_item table
            session.execute("truncate invoice_item");
            // Let's iterate through the java ResultSet
            while (rs.next()) {
                int invoice_number = rs.getInt("number");
                int invoice_item_id = rs.getInt("invoice_item_id");
                String service = rs.getString("service_description");
                int quantity = rs.getInt("quantity");
                double unit_value = rs.getDouble("unit_value");
                String source = rs.getString("name");
                String qualification = rs.getString("qualificatin_name");
                double tax_percent = rs.getDouble("tax_percent");
                double discount_percent = rs.getDouble("tax_percent");
                double subtotal = rs.getDouble("subtotal");

                session.execute("insert into invoice_item (number, invoice_item_id, service, quantity,"
                        + "unit_value, source, qualification, tax_percent, discount_percent, subtotal) values ("
                        + invoice_number
                        + "," + invoice_item_id
                        + ",'" + service + "'"
                        + "," + quantity
                        + "," + unit_value
                        + ",'" + source + "'"
                        + ",'" + qualification + "'"
                        + "," + tax_percent
                        + "," + discount_percent
                        + "," + subtotal + ")"
                );
            }
            rs.close();
            mysqlPrepareStat.close();
            if (mysqlConn != null) {
                mysqlConn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }

    }

}
