const mysql = require('mysql2');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const moment = require('moment');
const fs = require('fs');
const path = require('path');

// Criar a conexão com o banco de dados
const connection = mysql.createConnection({
    host: '127.0.0.1',
    user: 'root',
    password: '',
    database: 'teste_bkp'
});

// Conectar ao banco de dados
connection.connect((err) => {
    if (err) {
        return console.error('Erro ao conectar ao banco de dados: ' + err.message);
    }
    console.log('Conectado ao banco de dados MySQL.');
});

// Exemplo de consulta
connection.query('SELECT * FROM telemetria WHERE data BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 YEAR) AND CURDATE() ORDER BY data;', (err, results) => {
    if (err) {
        console.error('Erro ao executar a consulta: ' + err.message);
        return;
    }

    // Agrupar os resultados por mês e ano
    const groupedResults = results.reduce((acc, row) => {
        const monthYear = moment(row.DATA).format('YYYY-MM'); // Ex: "2023-11"
        if (!acc[monthYear]) {
            acc[monthYear] = [];
        }

        // Formatar a data no formato desejado
        row.DATA = moment(row.DATA).format('YYYY-MM-DD HH:mm:ss'); // Ex: "2023-09-01 00:00:00"

        acc[monthYear].push(row);
        return acc;
    }, {});

    // Criar uma pasta para armazenar os arquivos CSV
    const outputDir = 'output_csv';
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
    }

    // Salvar cada grupo em um arquivo CSV
    Object.keys(groupedResults).forEach(monthYear => {
        const filePath = path.join(outputDir, `${monthYear}.csv`);
        const headers = Object.keys(results[0] || {}).map(key => ({ id: key, title: key }));

        const csvWriter = createCsvWriter({
            path: filePath,
            header: headers,
            alwaysQuote: true,
            // fieldDelimiter: ';'
        });

        csvWriter.writeRecords(groupedResults[monthYear])
            .then(() => {
                console.log(`Arquivo CSV para ${monthYear} salvo com sucesso.`);
            })
            .catch(err => {
                console.error('Erro ao salvar o arquivo CSV: ' + err.message);
            });
    });

    // Fechar a conexão
    connection.end((err) => {
        if (err) {
            return console.error('Erro ao fechar a conexão: ' + err.message);
        }
        console.log('Conexão encerrada.');
    });
});