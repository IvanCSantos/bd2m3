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

// Cassandra https://github.com/rafaelqg/code/blob/main/cassandra_my_sql_integration.js
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

let loadDataToDatastax = async function (){
  con.connect(function(err) { if (err) throw err; console.log("Connection with mysql established");});
  await client.connect();
  
  // Questão 2a
  // await client.execute("DROP TABLE IF EXISTS employees.bymanager");
  // await client.execute("CREATE TABLE employees.bymanager (manager text, department text, employee text, PRIMARY KEY(manager, department, employee));");
  
  // let sql=`SELECT 
  //           concat(concat(e.first_name," "), e.last_name ) as Manager,
  //           d.dept_name as Department,
  //           concat(concat(e2.first_name," "), e2.last_name ) as Employee
  //         FROM dept_manager m
  //           INNER JOIN employees e ON (m.emp_no = e.emp_no)
  //           INNER JOIN departments d ON (m.dept_no = d.dept_no)
  //           LEFT JOIN dept_emp de ON (d.dept_no = de.dept_no)
  //           LEFT JOIN employees e2 ON (de.emp_no = e2.emp_no)
  //         WHERE m.to_date = '9999-01-01';`;

  // await con.query(sql, async function(err, result, fields) {
  //   if(err) throw err;

  //   await result.forEach(async record => {
  //     let sql ="INSERT INTO employees.bymanager (manager, department, employee)";
  //     sql+= ` VALUES('${record["Manager"]}','${record["Department"]}','${record["Employee"]}');`;
  //     //console.log(sql);
  //     const rs = await client.execute(sql);
  //     console.log(`Your cluster returned ${rs.rowLength} row(s)`);
  //   });
  // });
  // Fim Questão 2a

    // Questão 2b
  // await client.execute("DROP TABLE IF EXISTS employees.byDepartmentAndDate");
  // await client.execute("CREATE TABLE employees.byDepartmentAndDate (department text, from_date date, to_date date, employee text, PRIMARY KEY(department, from_date, to_date));");

  // sql=`SELECT
	//           d.dept_name as Department,
	//           DATE_FORMAT(de.from_date, '%Y-%c-%d') as FromDate,
	//           DATE_FORMAT(de.to_date, '%Y-%c-%d') as ToDate,
	//           concat(concat(e.first_name," "), e.last_name ) as Employee
  //         FROM dept_emp de
	//           INNER JOIN departments d ON (de.dept_no = d.dept_no)
	//           INNER JOIN employees e ON (de.emp_no = e.emp_no)`;

  // await con.query(sql, async function(err, result, fields) {
  //     if(err) throw err;
  //     await result.forEach(async record => {
  //       let sql ="INSERT INTO employees.byDepartmentAndDate (department, from_date, to_date, employee)";
  //       sql+= ` VALUES('${record["Department"]}','${record["FromDate"]}','${record["ToDate"]}','${record["Employee"]}');`;
  //       //console.log(sql);
  //       const rs = await client.execute(sql);
  //       console.log(`Your cluster returned ${rs.rowLength} row(s)`);
  //     });
  //   });
    // Fim Questão 2b

  // Questão 2c
  await client.execute("DROP TABLE IF EXISTS employees.averageWage");
  await client.execute("CREATE TABLE employees.averageWage (department text, employeeID int, birthDate date, firstName text, lastName text, gender text, hireDate date, salary float, PRIMARY KEY(department, employeeid));");
  
  sql=`SELECT
	          e.emp_no as EmployeeID,
            DATE_FORMAT(e.birth_date, '%Y-%c-%d') as BirthDate,
            e.first_name as FirstName,
            e.last_name as LastName,
            e.gender as Gender,
            DATE_FORMAT(e.hire_date, '%Y-%c-%d') as HireDate,
            d.dept_name as Department,
            s.salary as Salary
          FROM employees e 
            INNER JOIN dept_emp de ON (e.emp_no = de.emp_no)
            INNER JOIN departments d ON (de.dept_no = d.dept_no)
            INNER JOIN salaries s ON (e.emp_no = s.emp_no)
          WHERE de.to_date = '9999-01-01' AND s.to_date = '9999-01-01'`;

  await con.query(sql, async function(err, result, fields) {
    if(err) throw err;
    await result.forEach(async record => {
      let sql ="INSERT INTO employees.averageWage (department, employeeID, birthDate, firstName, lastName, gender, hireDate, salary)";
      sql+= ` VALUES('${record["Department"]}',${record["EmployeeID"]},'${record["BirthDate"]}','${record["FirstName"]}','${record["LastName"]}','${record["Gender"]}','${record["HireDate"]}',${record["Salary"]});`;
      console.log(sql);
      const rs = await client.execute(sql);
      console.log(`Your cluster returned ${rs.rowLength} row(s)`);
    });
  });
  // Fim Questão 2c
};

// 2a
let getEmployeesByManager = async function() {
  let query = prompt("Informe o nome do manager que deseja pesquisar: ")
  let sql = `SELECT * from employees.bymanager WHERE manager = '${query}';`;
  console.log(sql);
  const rs = await client.execute(sql);
  console.log(rs.rows);
}

// 2b
let getEmployeesByDepartmentAndDate = async function() {
  let query = prompt("Informe o nome do departamento que deseja pesquisar: ")
  let queryDate = prompt("Informe a data que deseja pesquisar ('YYYY-MM-DD'): ");
  let sql = `SELECT * from employees.byDepartmentAndDate WHERE department = '${query}' AND from_date <= '${queryDate}';`;
  console.log(sql);
  const rs = await client.execute(sql);
  console.log(rs.rows);
}

loadDataToDatastax();

const displayMenuOptions = function() {
  console.log("*** SELECIONE A OPÇÃO DESEJADA ***");
  console.log("1. Sincroniza dados do MySQL para o Cassandra");
  console.log("2. Retorna todos os employeers a partir do manager");
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
              loadDataToDatastax();
              break;
          case 2:
              getEmployeesByManager();
              break;
          case 3:
              await getEmployeesByDepartmentAndDate();
              break;
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