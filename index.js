const cheerio = require('cheerio')
const axios = require("axios")

const fetchData = async (siteUrl) => {
    const result = await axios.get(siteUrl)
    console.log(result.data.indexOf('Vihreää'))
    return cheerio.load(result.data)
};

const getText = async () => {
    const $ = await fetchData("https://twitter.com/i/web/status/1189845865671909376")
    const spanElements = $('article span')
    
    console.log(spanElements)
}

getText()