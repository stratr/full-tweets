const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const cheerio = require('cheerio')
const axios = require("axios")

// gcloud functions deploy fullTweets --env-vars-file .env.yaml --runtime nodejs10 --trigger-topic fetch_full_tweets --timeout 180s

const fetchData = async (siteUrl) => {
    const result = await axios.get(siteUrl)
    //console.log(result.data.indexOf('Mistä seuraavaksi'))
    //console.log(result.data.slice(4000, 5000))
    return cheerio.load(result.data)
}

const getTweetsToBeFetched = () => {
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        maxResults: 1000,
    };

    const bqTable = process.env.TWEETS_TO_FETCH_VIEW;

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

    return slicedTitle
}

const getTweets = async () => {
    const tweetUrlsBq = await getTweetsToBeFetched()
    const tweetUrls = tweetUrlsBq[0]

    console.log(tweetUrls)

    const htmlPromises = []
    tweetUrls.forEach(tweet => {
        const $ = fetchData(tweet.tweet_url)
        htmlPromises.push($)
    })

    Promise.all(htmlPromises)
        .then(htmls => {
            const fullTexts = []

            htmls.forEach(html => {
                const $ = html;
                const documentTitle = $('title').text()
                const fullText = sliceTitle(documentTitle)

                // replace the … in the end
                console.log(fullText)
            })
        })
        .catch(err => {
            console.log('Error in fetching the page.');
            console.log(err);
        });
}

getTweets()