const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const cheerio = require('cheerio')
const axios = require("axios")

// gcloud functions deploy fullTweets --env-vars-file .env.yaml --runtime nodejs10 --trigger-topic fetch_full_tweets --timeout 180s

const fetchData = async (siteUrl) => {
    const result = await axios.get(siteUrl)
    return {
        siteUrl: siteUrl,
        html: cheerio.load(result.data)
    }
    //return cheerio.load(result.data)
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

    const htmlPromises = []
    tweetUrls.forEach(tweet => {
        const tweetHtml = fetchData(tweet.tweet_url)
        htmlPromises.push(tweetHtml)
    })

    Promise.all(htmlPromises)
        .then(htmls => {
            const fullTexts = []

            htmls.forEach(html => {
                const $ = html.html;
                const documentTitle = $('title').text()
                const fullText = sliceTitle(documentTitle)

                fullTexts.push({
                    siteUrl: html.siteUrl,
                    fullText: fullText
                })
            })

            // filter texts to see if the actually are correct
            const fullTextsFiltered = fullTexts.filter(el => {
                const fullTextStart = el.fullText.slice(0,5);
                const foundInOriginal = tweetUrls.find(tweet => {
                    return tweet.text.slice(0,5) === fullTextStart
                })

                return foundInOriginal;
            })

            if (fullTextsFiltered.length < fullTexts.length) {
                console.log('Some sort of alert should be triggered. Fetched full text doesnt match original')
            }

            // TODO: push full texts to bigquery

            console.log('filtered')
            console.log(fullTextsFiltered)
        })
        .catch(err => {
            console.log('Error in fetching the page.');
            console.log(err);
        });
}

getTweets()