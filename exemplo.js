const express = require('express');//npm install express
const { Client } = require("cassandra-driver");
const mysql = require('mysql');
//initialize mysql connection
const MYSQL_IP="localhost";
const MYSQL_LOGIN="root";
const MYSQL_PASSWORD="root";
let con = mysql.createConnection({host:  MYSQL_IP,user: MYSQL_LOGIN,password: MYSQL_PASSWORD,database: "sakila"});
con.connect(function(err) { if (err) throw err; console.log("Connection with mysql established");});
 
const credentials_datastax = {
    "clientId": "YToWIPLMpDigFUfsLTFtIXHW",
    "secret": "mhmuUE9dEekoxiKr,Nm760WoKgiCK4mN3gM41DEtFCf3+zu4A5c0ubUNM+ErgZ.JKPEAq1vkH_bmRWfXKm4Hjy2C8XqnRFXRsFauJ722j1wJ8XPKKu9WxnS4.L7Ket0E",
    "token": "AstraCS:YToWIPLMpDigFUfsLTFtIXHW:5c9da7d66d2af25fb2dbd52e6fae8e25e3d12245f17bc5ec69fa5414b9046c30"
  }
 
const app = express();
app.listen(9037);//initialize web server
 // http://localhost:9037/get_customers_rentals
 
 
async function run() {
 
   
 
  const client = new Client({
    cloud: {
        secureConnectBundle: "secure-connect-news.zip",
    },
    credentials: {
        username: credentials_datastax.clientId,
        password: credentials_datastax.secret,
    },
  });
  app.get('/get_customers_rentals', async function (req, res) {
    await client.connect();
    const sql_select = "select * from news_ks.customers_rentals"; //where Customer = ?
    let query = sql_select;
    let parameters= [];//req.query.customer
    let result = await client.execute(query,parameters);
    console.log("total sync: ", result.rows.length);
    //CORS
    res.status(200);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods","POST,GET,OPTIONS,PUT,DELETE,HEAD");
    res.setHeader("Access-Control-Allow-Headers","X-PINGOTHER,Origin,X-Requested-With,Content-Type,Accept");
    res.setHeader("Access-Control-Max-Age","1728000");
    res.send(JSON.stringify(result.rows));
  });
 
  /*
  await client.execute("DROP TABLE IF EXISTS news_ks.customers_rentals");
  await client.execute("create table news_ks.customers_rentals (customer TEXT, rental_date timestamp,year INT, month INT, amount FLOAT,  PRIMARY KEY( (customer), year, month, rental_date))");
   ;
  const sql = `SELECT concat(concat(c.first_name," "), c.last_name) as "Customer", i.film_id, concat(concat(s.first_name," "), s.last_name) as "Staff", r.rental_id, r.rental_date, p.amount,
  YEAR(r.rental_date) as 'year', MONTH (r.rental_date) as 'month'
    FROM sakila.payment p
    inner join customer c on c.customer_id = p.customer_id
    inner join staff s on s.staff_id = p.staff_id
    inner join rental r on r.rental_id = p.payment_id
    inner join inventory i on r.inventory_id = i.inventory_id
   `;
 
  con.query(sql, function (err, result) {
        result.forEach(async record => {
            await client.connect();
            let sql ="insert into news_ks.customers_rentals (customer, rental_date, year, month, amount)";
            sql+= ` values('${record["Customer"]}','${new Date(record["rental_date"]).toISOString()}', ${record["year"]}, ${record["month"]}, ${record["amount"]})`;
            await client.execute(sql);
            //await client.shutdown();
        });
       
    });
*/
  // Execute a query
  //const rs = await client.execute("SELECT * FROM system.local");
  //console.log(`Your cluster returned ${rs.rowLength} row(s)`);
 //
}
 
// Run the async function
run();