from flask import Flask, render_template, request, url_for
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import datetime

app = Flask(__name__)


def get_date_range(start_date=None, end_date=None):
    if start_date is None or end_date is None:
        today = datetime.date.today()
        prev_sunday = today - datetime.timedelta(days=today.weekday() + 1)
        prev_monday = prev_sunday - datetime.timedelta(days=6)
        start_date = prev_monday.strftime('%Y-%m-%d')
        end_date = prev_sunday.strftime('%Y-%m-%d')
    return start_date, end_date


def upload_data():
    # implementation of upload_data function
    pass


def upload_db():
    # implementation of upload_db function
    pass


def replace_data():
    # implementation of replace_data function
    pass


# define the route and view function for the index page
@app.route('/', methods=['GET', 'POST'])
def index():
    start_date, end_date = get_date_range()

    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        language = request.form['language']
        message = f"The database is selected in {language}"
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date,
                               language=language)

    else:
        # create engine to connect to the database
        engine = create_engine('postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base')
        try:
            conn = engine.connect()
            message = "Digital data connected successfully"
            message_type = "success"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)
        except SQLAlchemyError:
            # if the connection fails, show an error message
            message = "Failed to connected digital data"
            message_type = "error"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)

if __name__ == '__main__':
    app.run(debug=True)
