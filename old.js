const db            = require('../../db'),
      DATABASE      = db.contracts,
      proxies       = db.proxies;

const Kucoin        = require('../../apis/kucoin-api.js'),
      client        = new Kucoin();

const Screener      = require('../../apis/screener.js'),
      screener      = new Screener;

const EventEmitter  = require('events'),
      event         = new EventEmitter();   

const functions     = require('../../modules/functions.js')

const arbitrageData = {
    exchange: 'kucoin',
    link: (symbol) => `https://www.kucoin.com/ru/trade/${symbol}-USDT`
}



const main = async (symbolData) => {
    try {

        const server = await functions.getProxyData(proxies)

        const blockchainDataPromises = symbolData.check.map(async(element) => {
            const data = await screener.getDexSymbolByContract(element.contract, element.network, server.ip)
            return data ? {element, data} : null
        })

        const blockchainDataResults = (await Promise.all(blockchainDataPromises)).filter(Boolean);
        if (!blockchainDataResults.length) return

        const getCexPrice = await client.getTicker(server.ip, symbolData.symbol)
        if (!getCexPrice) return

        blockchainDataResults.forEach(async(contractData) => {

            const calcArbitrage = functions.calculatePercentageDifference(getCexPrice.price, contractData.data?.priceUsd)
            const getBook = await client.getOrderBook(server.ip, symbolData.symbol, calcArbitrage.direction)
            if (!getBook) return
            
            const checkBook = functions.sumBook(getBook, contractData.data.priceUsd, calcArbitrage.direction)
            if (!checkBook) return
            
            if ((calcArbitrage.direction == 'cex' && !contractData.element.deposite) || (calcArbitrage.direction == 'dex' && !contractData.element.withdraw)) return

            if (calcArbitrage.percentageChange > functions.getSpread(contractData.element.network) && contractData.data?.liquidity?.usd > 30000 && contractData.data?.volume?.h24 > 3000) {
                
                return event.emit('signal', {
                    exchange: arbitrageData.exchange,
                    direction: calcArbitrage.direction,
                    percent: calcArbitrage.percentageChange,
                    orderBook: functions.getOrderBookUsdt(getBook, contractData.data.priceUsd, calcArbitrage.direction),
                    chain: contractData.data.chainId,
                    dexLink: contractData.data.url,
                    cexLink: arbitrageData.link(symbolData.symbol),
                    symbol: symbolData.symbol,
                    dexPrice: contractData.data.priceUsd,
                    cexPrice: getCexPrice.price.toString()
                })
            }
        })

    } catch (error) {
        return
    }
}


const startProcess = async() => {

    const server = await functions.getProxyData(proxies)
    if (!server) return console.log('no proxy')
    
    const symbolDB = await client.getSymbols(server.ip)
    const contractDB = await DATABASE.findAll({where: {exchange: arbitrageData.exchange}})

    if (!symbolDB || !contractDB) return setTimeout(startProcess, 5000)

    const allSymbols = await injectSymbolData(symbolDB, contractDB)

    if (!allSymbols.length) return setTimeout(startProcess, 5000)

    const parallelTasksLimit = Math.ceil(allSymbols.length / 15);
    console.log(`-------------------------------start kucoin-------------------------------${parallelTasksLimit}`)

    for (let i = 0; i < allSymbols.length; i += parallelTasksLimit) {

        const tasks = allSymbols.slice(i, i + parallelTasksLimit).map(async(symbol) => {
            return main(symbol)
        });
        
        await Promise.all(tasks);
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
    startProcess().catch(err => setTimeout(startProcess, 10000))

}

function injectSymbolData(symbolsData, contractsData) {

    const newArr = symbolsData.map(item => {
        const check = contractsData.filter(contract => contract.symbol == item.baseCurrency)
        return {symbol: item.baseCurrency, check}
    })

    const filteredArr = newArr.filter(({ check }) => check.length > 0);

    return filteredArr
}


setTimeout(startProcess, 5000)



module.exports = {event}
