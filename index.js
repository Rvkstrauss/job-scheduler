const express = require('express')
const app = express();
require('dotenv').config();

const cron = require('node-cron')
const _ = require("underscore");
require('dotenv').config();

const port = process.env.PORT;

// define tasks to be run periodically (based on types)
const type1Handler = (job) => {
    job.status = 'completed';
    // perform job type related actions
    //logic will adjust job status in jobRecord (e.g. 'running', 'completed', 'failed')
    return;
}

const type2Handler = (job) => {
    job.status = 'completed';
    // perform job type related actions
    //logic will adjust job status in jobRecord (e.g. 'running', 'completed', 'failed')
    return;
}
const jobHandlers = {
    type1: type1Handler,
    type2: type2Handler
}

const jobRecord = {};
const jobsInProgress = {};
deferred = [];

const Jobs = {
    addJob: function(job){
        job.id = uuid();
        
        if(_.contains(Object.keys(jobRecord), job.id)){
            return;
        } 
        job.status = 'queued';
        jobRecord[job.id] = job;
        if (jobsInProgress.length < process.env.JOB_QUEUE_LENGTH) {
            startJob(job.id);
        } else {
            jobRecord[job.id].status = 'deferred';
            deferred.push(job.id);
        }
    },
    startJob: function(job) {
        var now = Date.now();
        if(_.contains(Object.keys(jobRecord), job.id)){
            var handler = jobHandlers[jobRecord[id].type];
            
            if(job.interval) {
                jobRecord[job.id] = cron.schedule((job.interval), () => {
                    handler(job);
                });
            } else {
                handler(job);
            }
            job.status = 'scheduled';
            job.lastRun = now;
            jobRecord[job.id].lastRun = job;
            jobsInProgress[job.id] = job;
        }
    },
    cancelJob: function(id) {
        if(_.contains(Object.keys(jobRecord), job.id)){
            cron.destroy(jobRecord[job.id]);
            delete jobRecord[id];
            delete jobsInProgress[id];
        }
    },
    introspect: function(id) {
        if(_.contains(Object.keys(jobRecord), job.id)){
            const stats = {lastRun: jobRecord[id].lastRun, status: jobRecord[id].status}
            return stats;
        }
    }
};

const checkDeferredJobs = () => {
    deferred.forEach(job => {
        if (checkIfTaskIsDue(job.id, Date.now())) { jobs.startJob(job) };
    })
}

function checkIfTaskIsDue(id, invokeTime){
    const lastRun = jobRecord[id].lastRun || 0;

    const elapsedTime = invokeTime - lastRun;

    if(elapsedTime - job.interval * 1000 >= 0) {
        return true;
    }

    return false;   
}

app.get('/job', (req, res) => {
    //create job and it to record for introspection and scheduling
    Jobs.addJob(req.job);
    res.send(`job ${job.id} added to queue`)
})

app.get('/cancel', (req, res) => {
    //create job and it to record for introspection and scheduling
    Jobs.cancelJob(req.id);
    res.send(`job ${job.id} wiped from queue`)
})

app.get('/start', (req, res) => {
    //create job and it to record for introspection and scheduling
    Jobs.startJob(req.job);
    res.send(`job ${job.id} in queue started`)
})

app.get('/status', (req, res) => {
    const status = Jobs.introspect(req.id);
    res.send(`job ${job.id} status: ${status}`)
})

// start the server
app.listen(port, () => console.log(`Server: PORT ${port} active`));

setTimeout(() => {
    if (deferred.length > 0) {
        checkDeferredJobs(); 
    }
}, process.env.JOB_EXEC_INTERVAL);

