from flask import Flask, render_template, request, url_for
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import datetime

app = Flask(__name__)

def get_date_range():
    today = datetime.date.today()
    prev_sunday = today - datetime.timedelta(days=today.weekday() + 1)
    prev_monday = prev_sunday - datetime.timedelta(days=6)
    start_date = prev_monday.strftime('%Y-%m-%d')
    end_date = prev_sunday.strftime('%Y-%m-%d')
    return start_date, end_date

# define the route and view function for the index page
@app.route('/', methods=['GET', 'POST'])
def index():
    start_date, end_date = get_date_range()
    if request.method == 'POST':

        if request.form['action'] == 'upload_data':
            # code for uploading new data goes here

            message = "Data uploaded successfully"
            start_date, end_date = get_date_range()
            return render_template('index.html', message=message, start_date=start_date, end_date=end_date)

        elif request.form['action'] == 'upload_db':
            # code for uploading new data to the DB goes here

            message = "Data uploaded to DB successfully"
            start_date, end_date = get_date_range()
            return render_template('index.html', message=message, start_date=start_date, end_date=end_date)

        elif request.form['action'] == 'replace_data':
            # code for replacing data in the DB goes here

            message = "Data replaced in DB successfully"
            start_date, end_date = get_date_range()
            return render_template('index.html', message=message, start_date=start_date, end_date=end_date)

        start_date, end_date = get_date_range()
        return render_template('index.html', start_date=start_date, end_date=end_date)

    else:
        # create engine to connect to the database
        engine = create_engine('postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base')

        try:
            conn = engine.connect()
            message = "Data uploaded successfully"
            message_type = "success"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)
        except SQLAlchemyError:
            # if the connection fails, show an error message
            message = "Failed to upload data"
            message_type = "error"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)


if __name__ == '__main__':
    app.run(debug=True)