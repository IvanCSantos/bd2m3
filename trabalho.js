// PROMPT
const prompt = require('prompt-sync')({sigint: true}) // npm install --save prompt-sync

// Habilita / Desabilita debug no código
let debug = false;

// Express
const express = require('express');//npm install express

// MySQL
const { Sequelize, DataTypes} = require('sequelize'); //npm install --save sequelize , npm install --save mysql2
const MYSQL_IP="localhost";
const MYSQL_LOGIN="root";
const MYSQL_PASSWORD="a1b2c3d4";
const DATABASE = "employees"; // Usando modelo testdb https://github.com/datacharmer/test_db
const sequelize = new Sequelize(DATABASE , MYSQL_LOGIN, MYSQL_PASSWORD, {
  host: MYSQL_IP,
  dialect: "mysql",
  logging: debug
});

let mySQLConnTest = async function() {
    try {
        await sequelize.authenticate();
        console.log('Connection has been established successfully.');
    } catch (error) {
        console.error('Unable to connect to the MySQL database:', error);
    }
};
if(debug) mySQLConnTest();

// Cassandra
const { Client } = require("cassandra-driver");

// Modelo employess
const Employee = sequelize.define('Employee', {
  emp_no: { type: DataTypes.INTEGER, autoIncrement:true, primaryKey: true },
  birth_date: { type: DataTypes.DATEONLY },
  first_name: { type: DataTypes.STRING(14) },
  last_name: { type: DataTypes.STRING(16) },
  gender: { type: DataTypes.ENUM('M', 'F' )},
  hire_date: { type: DataTypes.DATEONLY }
}, { tableName: 'employees', timestamps: false });

const Salary = sequelize.define('Salary', {
  emp_no: { type: DataTypes.INTEGER, primaryKey: true },
  salary: { type: DataTypes.INTEGER },
  from_date: { type: DataTypes.DATEONLY, primaryKey: true },
  to_date: { type: DataTypes.DATEONLY, primaryKey: true }
}, { tableName: 'salaries', timestamps: false });
Employee.hasMany(Salary, { foreignKey: 'emp_no' });

const Title = sequelize.define('Title', {
  emp_no: { type: DataTypes.INTEGER, primaryKey: true },
  title: { type: DataTypes.STRING(50), primaryKey: true },
  from_date: { type: DataTypes.DATEONLY },
  to_date: { type: DataTypes.DATEONLY }
}, { tableName: 'titles', timestamps: false });
Employee.hasMany(Title, { foreignKey: 'emp_no' });

const Department = sequelize.define('Department', {
  dept_no: { type: DataTypes.CHAR(4), primaryKey: true },
  dept_name: { type: DataTypes.STRING(40) }
}, { tableName: 'departments', timestamps: false });

const DepartmentEmployee = sequelize.define('DepartmentEmployee', {
  emp_no: { type: DataTypes.INTEGER, primaryKey: true },
  dept_no: { type: DataTypes.CHAR(4), primaryKey: true },
  from_date: { type: DataTypes.DATEONLY },
  to_date: { type: DataTypes.DATEONLY }
}, { tableName: 'dept_emp', timestamps: false });
Employee.belongsToMany(Department, { through: DepartmentEmployee, foreignKey: 'emp_no'});
Department.belongsToMany(Employee, { through: DepartmentEmployee, foreignKey: 'dept_no'});

const DepartmentManager = sequelize.define('DepartmentManager', {
  emp_no: { type: DataTypes.INTEGER, primaryKey: true },
  dept_no: { type: DataTypes.CHAR(4), primaryKey: true },
  from_date: { type: DataTypes.DATEONLY },
  to_date: { type: DataTypes.DATEONLY }
}, { tableName: 'dept_manager', timestamps: false });
Department.belongsToMany(Employee, { through: DepartmentManager, foreignKey: 'dept_no'});
Employee.belongsToMany(Department, { through: DepartmentManager, foreignKey: 'emp_no'});

let migrateMySQLToCassandra = async function() {
  //await sequelize.authenticate();
  let employeeList = []
  try {
      let amount = await Employee.count();
      if (debug) console.log("Quantidade de registros encontrados na tabela employee: ", amount);
      let pages = Math.ceil(amount/1000);
      if (debug) console.log("Quantidade de páginas da paginação da consulta SQL: ", pages);

      for(let i = 0; i < pages; i++){
          let employees =  await Employee.findAll({
              include: [Title, Salary, Department],
              order: [['emp_no', 'ASC']],
              offset: i*1000,
              limit: 1000
          });
          employees.forEach(employee => {
              let newEmployeeObject = employee.dataValues;
              newEmployeeObject
              newEmployeeObject.titles = [];
              newEmployeeObject.salaries = [];
              newEmployeeObject.departments = [];

              newEmployeeObject.Titles.forEach(title => {
                  delete title.dataValues.emp_no;
                  newEmployeeObject.titles.push(title.dataValues);
              });

              newEmployeeObject.Salaries.forEach(salary => {
                  delete salary.dataValues.emp_no;
                  newEmployeeObject.salaries.push(salary.dataValues);
              });

              newEmployeeObject.Departments.forEach(department => {
                  delete department.dataValues.DepartmentEmployee;
                  newEmployeeObject.departments.push(department.dataValues);
              });

              delete newEmployeeObject.Titles;
              delete newEmployeeObject.Salaries;
              delete newEmployeeObject.Departments;
              if (debug) console.log(newEmployeeObject);
              employeeList.push(newEmployeeObject);
          });
          //insertMongoData(employeeList);
          employeeList = [];
          console.log(`Executando página ${i}/${pages}`);
      }
      //sequelize.close();
  } catch (error) { 
      console.error("Error log", error);
  };
};

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
      displayMenuOptions()
      option = parseInt(prompt("Selectione uma opção do menu: "));
      if(option < 1 || option > 6) {
          console.log("Escolha uma opção entre 1 e 6");
          break;
      }
      switch(option) {
          case 1:
              //await deleteEmployeeCollection();
              await migrateMySQLToCassandra();
              //await createMongoIndexes();
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
menu();
 
/* const app = express();
app.listen(9037);//initialize web server
 // http://localhost:9037/get_customers_rentals
 
 
async function run() {
  let authProvider = new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra');
  const keyspace="employees";
  let contactPoints = ['localhost'];
  const client = new Client({
    contactPoints: contactPoints,
    keyspace: keyspace, localDataCenter: 'datacenter1'
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

  // Execute a query
  //const rs = await client.execute("SELECT * FROM system.local");
  //console.log(`Your cluster returned ${rs.rowLength} row(s)`);
 //
}
 
// Run the async function
run(); 
*/

