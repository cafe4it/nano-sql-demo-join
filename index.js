const express = require('express');
const fetch = require('node-fetch');
const Bluebird = require('bluebird');
const nSQL = require('nano-sql').nSQL;
const _ = require('lodash');

let app = express();
fetch.Promise = Bluebird;


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

nSQL('Reports_Albums').model([
    {key: 'id', type: 'string', props: ['pk']},
    {key: 'downloadedDate', type: 'int'},
    {key: 'albumId', type: 'int'},
    {key: 'totalDownload', type: 'int'}
])

nSQL('Reports_Photos').model([
    {key: 'id', type: 'string', props: ['pk']},
    {key: 'viewedDate', type: 'int'},
    {key: 'photoId', type: 'int'},
    {key: 'totalViews', type: 'int'}
])

function generateReport(today){
    today = today || (new Date()).setHours(0,0,0,0)
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
                        nSQL('Albums').query('upsert', album).exec()
                    })
                    console.log('seed Albums done!')
                })
        }
    })

    nSQL('Photos').query('select').exec().then((rows) => {
        if (rows.length <= 0) {
            fetch('https://jsonplaceholder.typicode.com/photos')
                .then((res) => res.json())
                .then((data) => {
                    _.each(data, (d) => {
                        nSQL('Photos').query('upsert', d).exec()
                    })
                    console.log('seed Photos done!')
                })
        }
    })

    const tables = ['Reports_Albums', 'Reports_Photos']
    _.each(tables, (t) => {
        nSQL(t).query('select').exec().then((rows) => {
            if (rows.length <= 0) {
                for (let i = -100; i < 0; i++) {
                    const today = new Date();
                    today.setDate(i)
                    today.setHours(0, 0,0,0)
                    for(let j = 1; j <= 1000; j++){
                        let report = {}
                        if(t === 'Reports_Albums'){
                            report = {
                                albumId: _.random(1, 50),
                                totalDownload: _.random(10, 1000),
                                downloadedDate: today.getTime()
                            }
                            report.id = `${report.downloadedDate}!${report.albumId}`
                        }else{
                            report = {
                                photoId: _.random(1, 5000),
                                totalViews: _.random(10, 1000),
                                viewedDate: today.getTime()
                            }
                            report.id = `${report.viewedDate}!${report.photoId}`
                        }
                        nSQL(t).query('upsert', report).exec()
                    }
                }
            }
        })
        console.log(`seed ${t} done!`)
    })


    app.get('/', function (req, res) {
        try {
            const cmd = req.query['cmd']
            const limit = req.query['limit'] || 10
            const offset = req.query['limit'] || 0
            if(cmd){
                switch (cmd) {
                    case 'summary':
                        const tables = ['Albums', 'Photos', 'Reports_Albums', 'Reports_Photos']
                        const promises = _.map(tables, (t) => {
                            return nSQL(t).query('select', [`COUNT(*) AS ${t}.records`]).exec()
                        })
                        Promise.all(promises).then((data) => {
                            res.json(_.chain(data).flatten().reduce((a, b) => _.assign(a, b)).value())
                        })
                        break;
                    case 'top_albums_downloaded':
                        nSQL('Reports_Albums')
                            .query('select', ['Albums.id AS id', 'Albums.title AS title', 'SUM(Reports_Albums.totalDownload) AS totalDownload'])
                            .join({
                                type: 'inner',
                                table: 'Albums',
                                where: ['Reports_Albums.albumId', '=', 'Albums.id']
                            })
                            .groupBy({'Albums.id': 'asc'})
                            .orderBy({'totalDownload': 'desc'})
                            .limit(limit)
                            .offset(offset)
                            .exec()
                            .then((rows) => {
                                res.json(rows)
                        })
                        break;
                    case 'top_photos_views':
                        nSQL('Reports_Photos')
                            .query('select', ['Photos.*', 'SUM(Reports_Photos.totalViews) AS totalViews'])
                            .join({
                                type: 'inner',
                                table: 'Photos',
                                where: ['Reports_Photos.photoId', '=', 'Photos.id']
                            })
                            .groupBy({'Photo.id': 'asc'})
                            .orderBy({'totalViews': 'desc'})
                            .limit(limit)
                            .offset(offset)
                            .exec()
                            .then((rows) => {
                                res.json(rows)
                            })
                        break;
                }
            }


        } catch (e) {
            console.error(e)
        }
    })

    const port = 3000;
    app.listen(port, function () {
        console.log('App listening on port:' + port);
    })
})
