const mysql = require('mysql');
const MYSQL_IP="localhost";
const MYSQL_LOGIN="root";
const MYSQL_PASSWORD="a1b2c3d4";
const DATABASE = "employees"; // Usando modelo testdb https://github.com/datacharmer/test_db
let con = mysql.createConnection({host: MYSQL_IP, user: MYSQL_LOGIN, password: MYSQL_PASSWORD, database: "employees"});
con.connect(function(err) { 
    if (err) throw err; 
    console.log("Connection with mysql established");
});

let connectMySQL = async function (){
    await con.query("select * from employees;", function(err, result, fields) {
        console.log("Entrou no con.query");
        if(err) throw err;
        console.log("Result: " + fields);
    });
}
connectMySQL();