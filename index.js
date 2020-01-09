const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const cheerio = require('cheerio')
const axios = require("axios")

const dataset = bigquery.dataset(process.env.BQ_DATASET);
const table = dataset.table(process.env.BQ_TABLE);
console.log('Dataset: ' + process.env.BQ_DATASET);
console.log('Table: ' + process.env.BQ_TABLE);

// gcloud functions deploy fullTweets --env-vars-file .env.yaml --runtime nodejs10 --trigger-topic fetch_full_tweets --timeout 180s
// node -e 'require("./index").fullTweets()'

const fetchData = async (siteUrl) => {
    const result = await axios.get(siteUrl, {
        validateStatus: function(status) {
            return status === 404 || (status >= 200 && status < 300); // Accept 404 (tweets that have been deleted)
        }
    })
    return {
        siteUrl: siteUrl,
        html: cheerio.load(result.data),
        status: result.status
    }
}

const getTweetsToBeFetched = () => {
    const options = {
        maxResults: 1000,
    };

    const bqTable = process.env.TWEETS_TO_FETCH_VIEW; // count of tweets to be fetched is determined in the BQ view

    const query = "SELECT * FROM " + bqTable;

    return bigquery.query(query, options);
}

const sliceTitle = (documentTitle) => {
    const start = documentTitle.indexOf('"') + 1
    const end = documentTitle.lastIndexOf('"')
    const slicedTitle = documentTitle.slice(start, end)

    if (/…\shttps?:\/\/t.+$/.test(slicedTitle)) {
        return slicedTitle.replace(/…([^…]*)$/, "$1")
    }

    return slicedTitle;
}

function insertRowsAsStream(rows) {
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        raw: true,
        allowDuplicates: false
    };

    return table.insert(rows, options);
}

function bigQueryMapper(tweets) {
    var rows = tweets.map(tweet => {
        return {
            "insertId": tweet.siteUrl,
            "json": {
                siteUrl: tweet.siteUrl,
                fullText: tweet.fullText
            }
        };
    });
    return rows;
}

const getTweets = async () => {
    const tweetUrlsBq = await getTweetsToBeFetched()
    const tweetUrls = tweetUrlsBq[0]
    console.log('Got ' + tweetUrls.length + ' URLs from BigQuery.')
    console.log(tweetUrls)

    const htmlPromises = [];
    tweetUrls.forEach(tweet => {
        const tweetHtml = fetchData(tweet.tweet_url)
        htmlPromises.push(tweetHtml)
    });

    return Promise.all(htmlPromises)
        .then(htmls => {
            const fullTexts = []

            htmls.forEach(html => {
                const $ = html.html;
                const documentTitle = $('title').text()
                const fullText = html.status === 404 ? '404' : sliceTitle(documentTitle)
                console.log(fullText)

                fullTexts.push({
                    siteUrl: html.siteUrl,
                    fullText: fullText
                })
            })

            const bqRows = bigQueryMapper(fullTexts)
            if (bqRows.length > 0) {
                console.log('Inserting ' + bqRows.length + ' records in BigQuery.')
                insertRowsAsStream(bqRows)
            } else {
                console.log('No new tweet texts to insert. Returning.');
                return;
            }
        })
        .catch(error => {
            if (error.response) {
                // The request was made and the server responded with a status code
                // that falls out of the range of 2xx
                console.log(error.response.data);
                console.log(error.response.status);
                console.log(error.response.headers);
            } else if (error.request) {
                // The request was made but no response was received
                // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
                // http.ClientRequest in node.js
                console.log(error.request);
            } else {
                // Something happened in setting up the request that triggered an Error
                console.log('Error', error.message);
            }
            console.log(error.config);
        });
}

/*
This is the function that is triggered by Pub/Sub
*/
exports.fullTweets = (data) => {
    return getTweets(data);
};