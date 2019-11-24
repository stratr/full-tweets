const cheerio = require('cheerio')
const axios = require("axios")

const fetchData = async (siteUrl) => {
    const result = await axios.get(siteUrl)
    //console.log(result.data.indexOf('Mistä seuraavaksi'))
    //console.log(result.data.slice(4000, 5000))
    return cheerio.load(result.data)
};

const getText = async () => {
    const $ = await fetchData("https://twitter.com/i/web/status/1189845865671909376")
    const documentTitle = $('title').text()

    const start = documentTitle.indexOf('"')
    const end = documentTitle.lastIndexOf('"')
    
    console.log(documentTitle.slice(start, end))
}

getText()