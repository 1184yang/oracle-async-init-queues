const oradb = require('./database');

oradb.initialize();
oradb.on('initialized', () => console.log('Database initialized.'));

const statement = 'select * from emp where empno = :empno';

async function doExecute(dbpool, statement, binds) {  
  try {
    const result = await oradb.doExecute(dbpool, statement, binds);
    console.log(result);
  } catch (err) {
    console.log(err);
  } 
}

async function doExecuteMany(dbpool, statement, binds) {  
  try {
    const result = await oradb.doExecuteMany(dbpool, statement, binds);
    console.log(result);
  } catch (err) {
    console.log(err);
  } 
}

async function employees() {
  const dbpool = "xedemo";
  const statement = 'select * from emp';
  try {
    const result = await oradb.doExecute(dbpool, statement);
    console.log(result);
  } catch (err) {
    console.log(err);
  } 
}

doExecute("xedemo", statement, [7566]);

setTimeout(() => {
  doExecute("xedemo", statement, [7782]);
  //employees();
}, 600);

setTimeout(() => oradb.close(), 30000);