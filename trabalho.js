// PROMPT
const prompt = require('prompt-sync')({sigint: true}) // npm install --save prompt-sync

// Habilita / Desabilita debug no código
let debug = false;

// Express
const express = require('express');//npm install express

// MySQL
const mysql = require('mysql');
const MYSQL_IP="localhost";
const MYSQL_LOGIN="root";
const MYSQL_PASSWORD="a1b2c3d4";
const DATABASE = "employees"; // Usando modelo testdb https://github.com/datacharmer/test_db
let con = mysql.createConnection({host: MYSQL_IP, user: MYSQL_LOGIN, password: MYSQL_PASSWORD, database: "employees"});
con.connect(function(err) { if (err) throw err; console.log("Connection with mysql established");});

// Cassandra https://github.com/rafaelqg/code/blob/main/cassandra_my_sql_integration.js
//const cassandra  = require("cassandra-driver");
// let authProvider = new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra');
// let contactPoints = ['localhost'];
// let localDataCenter = 'DataCenter1';
// let client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, localDataCenter: localDataCenter, keyspace:'employees'});
let cassandra = require("cassandra-driver");
const credentials_datastax = {
  "clientId": "yuQwSfDitkGDHUcEkLMEzDgP",
  "secret": "WSw.,x7EzbgtD4Re,CnTN0f4oXgITttdZI-,HSgISLWs5Q3BZJpk+b5gREjdP4sp5Y6+LB1Wm,,aSrtrPqw8P6ufZmjP+X.u,ThZP9NwqExFG-9iKBX_AqQKkEwsDM-p",
  "token": "AstraCS:yuQwSfDitkGDHUcEkLMEzDgP:7f223542a99c3e1e62419313b6369da69c961fd8ce2aa9a0ac9b862b67dc8820"
}
const client = new cassandra.Client({
  cloud: {
      secureConnectBundle: "secure-connect-cassandra.zip",
  },
  credentials: {
      username: credentials_datastax.clientId,
      password: credentials_datastax.secret,
  },
  options: {
    pooling: {
      maxRequestsPerConnection: 2048,
    }
  }
});

// async function migrateMySQLToCassandra() {
//   // Keyspace management is currently only supported at https://astra.datastax.com/org/37a269a1-f0dc-4474-8128-59408fa9c61d/database/68d34aa9-647d-4d89-bc44-dc65dc668175
//   // await client.connect(function(e) {
//   //   let query = "CREATE KEYSPACE IF NOT EXISTS employees WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
//   //   return client.execute(query, function(e, res) {
//   //     return console.log(e, res);
//   //   });
//   // });
  
//   console.log("Chegou no drop table if exists");
//   await client.execute("DROP TABLE IF EXISTS employees.bymanager");
//   await client.execute("CREATE TABLE employees.bymanager (manager text PRIMARY KEY, department text, employee text);");

//   console.log("Chegou na instrução de montar a query SQL")

//   let sql=`SELECT concat(concat(e.first_name," "), e.last_name ) as Manager,
// 	      d.dept_name as Department,
// 	      concat(concat(e2.first_name," "), e2.last_name ) as Employee
//         FROM dept_manager m
// 	      INNER JOIN employees e ON (m.emp_no = e.emp_no)
// 	      INNER JOIN departments d ON (m.dept_no = d.dept_no)
// 	      LEFT JOIN dept_emp de ON (d.dept_no = de.dept_no)
// 	      LEFT JOIN employees e2 ON (de.emp_no = e2.emp_no)
//         WHERE m.to_date = '9999-01-01'`;

//   console.log("Chegou na query de insert");

// //   con.query(sql, function (err, result) {
// //     console.log("Entrou na função con.query");
// //     let count = 0;
// //     result.forEach(async record => {
// //       console.log(count++, "..");
// //       await client.connect();
// //       let sql ="INSERT INTO employees.bymanager (manager, department, employee)";
// //       sql+= ` VALUES('${record["Manager"]}','${record["Department"]}',${record["Employee"]}`;
// //       await client.execute(sql);
// //     });
// // });
//   //await con.connect(async function (err, result, fields) {
//     //console.log(err, result, fields);
//     //if (err) throw err;
//     await con.query("select * from employees;", function(err, result, fields) {
//       console.log("Entrou no con.query");
//       if(err) throw err;
//       console.log(result, fields);
//     });
//   //});
// }

let loadDataToDatastax = async function (){
  await client.connect();
  await client.execute("DROP TABLE IF EXISTS employees.bymanager");
  await client.execute("CREATE TABLE employees.bymanager (employeeID int, manager text, department text, employee text, PRIMARY KEY(employeeID,manager));");
  
  // Questão 2a
  let sql=`SELECT 	e2.emp_no as EmployeeID,
          concat(concat(e.first_name," "), e.last_name ) as Manager,
          d.dept_name as Department,
          concat(concat(e2.first_name," "), e2.last_name ) as Employee
          FROM dept_manager m
          INNER JOIN employees e ON (m.emp_no = e.emp_no)
          INNER JOIN departments d ON (m.dept_no = d.dept_no)
          LEFT JOIN dept_emp de ON (d.dept_no = de.dept_no)
          LEFT JOIN employees e2 ON (de.emp_no = e2.emp_no)
          WHERE m.to_date = '9999-01-01';`;

  await con.query(sql, async function(err, result, fields) {
      if(err) throw err;
      //console.log("Result: " + JSON.stringify(result));
      let count = 0;
      await result.forEach(async record => {
        console.log(++count);
        let sql ="INSERT INTO employees.bymanager (employeeID, manager, department, employee)";
        sql+= ` VALUES(${record["EmployeeID"]},'${record["Manager"]}','${record["Department"]}','${record["Employee"]}');`;
        console.log(sql);
        await client.connect();
        const rs = await client.execute(sql);
        console.log(`Your cluster returned ${rs.rowLength} row(s)`);
      });
    });
    // Fim Questão 2a
    
  //await client.shutdown();
};

loadDataToDatastax();

const displayMenuOptions = function() {
  console.log("*** SELECIONE A OPÇÃO DESEJADA ***");
  console.log("1. Sincroniza dados do MySQL para o Cassandra");
  console.log("2. Consulta funcionário a partir de id do manager");
  console.log("3. Consulta funcionário a partir de um title");
  console.log("4. Consulta funcionário a partir de um departamento");
  console.log("5. Relatório de média salarial de funcionários por departamento");
  console.log("6. Sair");
  console.log("");
}
async function menu() {
  let option = 0
  while(option !== 6){
      displayMenuOptions();
      option = parseInt(prompt("Selectione uma opção do menu: "));
      if(option < 1 || option > 6) {
          console.log("Escolha uma opção entre 1 e 6");
          break;
      }
      switch(option) {
          case 1:
              await connectMySQL();
              break;
          case 2:
              let promptManager = prompt("Qual gerente deseja consultar? ");
              await getEmployeeByManager(promptManager);
              break
          case 3:
              let promptTitle = prompt("Qual title deseja consultar? ");
              await getEmployeeByTitles(promptTitle);
              break
          case 4:
              let promptDepartment = prompt("Qual departamento deseja consultar? ");
              await getEmployeeByDepartment(promptDepartment);
              break
          case 5:
              await getDepartmentsAverageWage();
              break
          case 6:
              console.log(typeof(option),": ", option);
          default:
              console.log(`Opção inválida ${option}!`);
      }
  }
}
//menu();