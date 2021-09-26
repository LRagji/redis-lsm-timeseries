//This is just a sample microservice
const express = require('express')
const compression = require('compression')
const app = express()
const port = 3000

app.use(express.json());

app.use(compression());

app.post('/set', async (req, res) => {
    await new Promise((acc, rej) => setTimeout(acc, 2000));
    res.status(204).end();
});

app.post('/get', async (req, res) => {
    await new Promise((acc, rej) => setTimeout(acc, 2000));
    res.json({});
});

//Startup
app.listen(port, () => {
    console.log(`App listening at http://localhost:${port}`);
});