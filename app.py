from flask import Flask, render_template
from database import load_jobs_from_db, load_jobs_as_json

app = Flask(__name__)


@app.route("/")
def hello_world():
  jobs = load_jobs_from_db()
  return render_template('home.html', jobs=jobs, company_name='Jovian')


@app.route("/api/jobs")
def list_jobs():
  return load_jobs_as_json()


if __name__ == "__main__":
  app.run(host="0.0.0.0", debug=True)
