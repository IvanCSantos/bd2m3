// Instala o cassandra no docker: docker run -d --name cassandra -v /Users/ivansantos/Dev/UNIVALI/BancoDadosII/cassandra/datadir:/var/lib/cassandra -p 9042:9042 cassandra
// Conecta no container do cassandra: docker exec -it cassandra /bin/bash
// Abre o programa de conexão com o banco de dados: cqlsh
// Exibir os keyspaces: describe keyspaces;
// create keyspace if not exists teste with replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
// create table if not exists teste.alunos ( id int primary key, name text );
// insert into teste.alunos (id,name) values (1,'Ivan');

let cassandra = require('cassandra-driver');
// Código do professor no slide 216
// let authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');
// const keyspace="teste";
// let contactPoints = ['localhost'];
// let client = new cassandra.Client({
//     contactPoints: contactPoints,
//     keyspace:keyspace, localDataCenter: 'datacenter1'
// });

// //let query = 'select * from teste where firstname=? and lastname=?';
// let query = 'select * from teste where name=?';
// //let parameters=["Rafael", "Gonçalves"];
// let parameters=["1"];
// client.execute(query, parameters, function(error, result){
//     if(error!=undefined){
//         console.log('Error:', error);
//     } else {
//         console.table(result.rows);
//     }
// })

async function run() {
    const credentials_datastax = {
        "clientId": "string",
        "secret": "string",
        "token": "string"
    }

    const client = new Client({
        cloud: {
            secureConnectBundle: "secure-connect-news.zip",
        },
        credentials: {
            username: credentials_datastax.clientId,
            password: credentials_datastax.secret
        },
    });

    await client.connect();

    // execute a query
    const rs = await client.execute("SELECT * FROM system.local");
    console.log(`Your cluster returned ${rs.rowLength} row(s)`);
    await client.shutdown();
}
run();