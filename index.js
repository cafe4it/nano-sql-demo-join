const express = require('express');
const fetch = require('node-fetch');
const Bluebird = require('bluebird');
const nSQL = require('nano-sql').nSQL;
const _ = require('lodash');

let app = express();
fetch.Promise = Bluebird;

const today = new Date()
today.setHours(0, 0, 0, 0)

nSQL('Albums').model([
    {key: 'id', type: 'int', props: ['pk']},
    {key: 'title', type: 'string'}
])

nSQL('Photos').model([
    {key: 'id', type: 'int', props: ['pk']},
    {key: 'title', type: 'string'},
    {key: 'url', type: 'string'},
    {key: 'thumbnailUrl', type: 'string'},
    {key: 'albumId', type: 'int'}
])

nSQL('Reports').model([
    {key: 'id', type: 'string', props: ['pk']},
    {key: 'downloadedDate', type: 'int'},
    {key: 'albumId', type: 'int'},
    {key: 'totalDownload', type: 'int'}
])

function generateReport(){
    const report = {
        albumId: _.random(1, 50),
        totalDownload: _.random(10, 1000),
        downloadedDate: today.getTime()
    }
    report.id = `${report.downloadedDate}!${report.albumId}`
    return report;
}

nSQL().config({
    id: 'myDB',
    mode: 'PERM'
}).connect().then((result) => {
    nSQL('Albums').query('select').exec().then((rows) => {
        if (rows.length <= 0) {
            fetch('https://jsonplaceholder.typicode.com/albums')
                .then((res) => res.json())
                .then((data) => {
                    _.each(data, (d) => {
                        const album = _.omit(d, 'userId')
                        nSQL('Albums').query('upsert', album).exec().then((rows) => console.log(rows))
                    })
                })
        }
    })

    nSQL('Photos').query('select').exec().then((rows) => {
        if (rows.length <= 0) {
            fetch('https://jsonplaceholder.typicode.com/photos')
                .then((res) => res.json())
                .then((data) => {
                    _.each(data, (d) => {
                        nSQL('Photos').query('upsert', d).exec().then((rows) => console.log(rows))
                    })
                })
        }
    })


    nSQL('Reports').query('select').exec().then((rows) => {
        if (rows.length <= 0) {
            for (let i = 0; i <= 50; i++) {
                const report = generateReport();
                nSQL('Reports').query('upsert', report).exec().then(rows => console.log(rows))
            }
        }
    })

    app.get('/', function (req, res) {
        try {
            nSQL('Reports')
                .query('select', ['Albums.id AS id', 'Albums.title AS title', 'Reports.totalDownload AS totalDownload'])
                .join({
                    type: 'inner',
                    table: 'Albums',
                    where: ['Reports.albumId', '=', 'Albums.id']
                }).orderBy({'totalDownload': 'desc'}).exec().then((rows) => {
                res.json(rows)
            })
        } catch (e) {
            console.error(e)
        }
    })

    const port = 3000;
    app.listen(port, function () {
        console.log('App listening on port:' + port);

        //simulator users download albums
        // setInterval(function () {
        //     const report = generateReport();
        //     nSQL('Reports').query('upsert', report).exec().then(() => console.log('User downloading...album ' + report.albumId))
        // }, _.random(500, 5000))
    })
})
