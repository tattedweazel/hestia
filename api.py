from flask import Flask, request

app = Flask(__name__)


# /jobs/<jobname>?target_date=<target_date>&dry_run=<dry_run>'
@app.route('/jobs/<jobname>', methods=['GET', 'POST'])
def run_job(jobname):
    target_date = request.args.get('target_date')
    dry_run = request.args.get('dry_run')
    j = __import__(f'api.views.jobs.{jobname}', globals(), locals(), ['run'], 0)
    content = {
        "target_date": target_date,
        "dry_run": dry_run
    }
    j.run(content)
    return {"status": 200}


# /process_handlers/<process_handler>?local_mode=<local_mode>&target_date=<target_date>&dry_run=<dry_run>'
@app.route('/process_handlers/<process_handler>', methods=['GET', 'POST'])
def run_process_handler(process_handler):
    local_mode = request.args.get('local_mode')
    target_date = request.args.get('target_date')
    dry_run = request.args.get('dry_run')
    j = __import__(f'api.views.process_handlers.{process_handler}', globals(), locals(), ['run'], 0)
    content = {
        "local_mode": local_mode,
        "target_date": target_date,
        "dry_run": dry_run
    }
    j.run(content)
    return {"status": 200}



if __name__ == '__main__':
    app.run(debug=True)
