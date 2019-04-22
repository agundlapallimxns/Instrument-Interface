from flask import *
import snowflake.connector  as sql
import sqlalchemy as ssql
import os
import pandas as pd

app = Flask(__name__)
app.secret_key = os.urandom(24)

@app.route('/')
def main():
    return render_template('index.html')


@app.route('/showSignin')
def showSignin():
    return render_template('signin.html')


@app.route('/dashboard')
def showDashboard():
    return render_template('dashboard.html')


@app.route('/validateLogin', methods=['GET', 'POST'])
def validateLogin():
    try:
        _username = request.form['username']
        _password = request.form['inputPassword']
        # connect to db
        query = "select * from abhi_user_tbl where user_name ='" + _username + "'"
        data=getDataFromDB(query)
        row = data.fetchone()
        if(row[1] ==_password):
            session['user'] = row[0]
            return redirect('/userHome')
        else:
            return render_template('index.html')
    except Exception as e:
        return render_template('index.html')

@app.route('/userHome')
def userHome():
    return render_template('userhome.html')


@app.route('/showAddInstrument')
def showAddInstrument():
    return render_template('addInstrument.html')


@app.route('/InsertInstrument')
def InsertInstrument():
    return 1


def getDataFromDBSnoflake(sqlQuery):
    snflakeURL='snowflake://abhinav.gundlapalli:Limssnowflake1234@mxns.snowflakecomputing.com/SANDBOX/ABHINAV'
    engine = ssql.create_engine(snflakeURL)
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()


def getDataFromDB(queryText):
    dbUser='LIMS'
    dbPassword='LIMSd3v1'
    dbHost='172.31.6.130'
    dbPort='1521'
    dbName='dev1'
    dburl = 'oracle://' + dbUser + ':' + dbPassword + '@' + dbHost + ':' + dbPort + '/' + dbName
    # print(dburl)
    engine = ssql.create_engine(dburl)
    conn = engine.connect()
    results = conn.execute(queryText)
    return results


if __name__ == "__main__":
    app.run()