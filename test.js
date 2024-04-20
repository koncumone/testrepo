const   db            = require('../../db'),
        contracts     = db.contracts,
        proxies       = db.proxies;

const   Kucoin        = require('../../apis/kucoin-api.js'),
        client        = new Kucoin();

const   Screener      = require('../../apis/screener.js'),
        screener      = new Screener;

const   EventEmitter  = require('events'),
        event         = new EventEmitter();   

const   functions     = require('../../modules/functions.js')

const   arbitrageData = {
        exchange: 'kucoin',
        link: (symbol) => `https://www.kucoin.com/ru/trade/${symbol}-USDT`
}


// const emitSignal = () => {


//     event.emit('signal', {
//         exchange: arbitrageData.exchange,
//         direction: percentData.direction,
//         percent: percentData.percentageChange,
//         orderBook: functions.getOrderBookUsdt(orderBook, dex.data.priceUsd, percentData.direction),
//         chain: dex.data.network,
//         dexLink: dex.data.url,
//         cexLink: arbitrageData.link(symbolData.symbol),
//         symbol: dex.contract.symbol,
//         dexPrice: dex.data.priceUsd,
//         cexPrice: cex.price.toString(),
//         buyTax: dex.contract.buyTax,
//         sellTax: dex.contract.sellTax,
//         scam: dex.contract.scam
//     })
// }

const processSpread = (percentData, data, orderBook) => {

    const {deposite, withdraw, buyTax, sellTax, network} = data.contract

    const checkDeposites    = (percentData.direction == 'cex' && deposite) || (percentData.direction == 'dex' && withdraw)

    const liquidity         = (data.data?.liquidity?.usd > 30000) && (data.data?.volume?.h24 > 3000)

    const bookLiquidity     = functions.sumBook(orderBook, data.data.priceUsd, percentData.direction)

    const spread            = percentData.percentageChange > (parseInt(buyTax) + parseInt(sellTax) + functions.getSpread(network))

    return checkDeposites && liquidity && spread && bookLiquidity
}

const checkArbitrage = async(server, dex, cex) => {

    if (!server || !dex || !cex) return 

    const percentData = functions.calculatePercentageDifference(cex.price, dex.data?.priceUsd)
    const orderBook = await client.getOrderBook(server.ip, dex.contract.symbol, percentData.direction)

    const checkSpread = processSpread(percentData, dex, orderBook)

    if (checkSpread)
        return emitSignal()
}


const processArbitrage = async(symbolData) => {

    const server = await functions.getProxyData(proxies)

    const [blockChainData, cexData] = await Promise.all([
        getBlockchainData(symbolData, server),
        client.getTicker(server.ip, symbolData.symbol)
    ])

    blockChainData.forEach(async(blockchain) => {
        await checkArbitrage(server, blockchain, cexData)
    });
}

const getSymbolsArr = async() => {

    const server = await functions.getProxyData(proxies)

    const [symbolsArr, contractsArr] = await Promise.all([
        client.getSymbols(server.ip),
        contracts.findAll({where: {exchange: arbitrageData.exchange}})
    ])

    const newArr = symbolsArr.map(item => {
        const check = contractsArr.filter(contract => contract.symbol == item.baseCurrency)
        return { symbol: item.baseCurrency, check }
    })

    return newArr.filter(({ check }) => check.length > 0)
}


const getBlockchainData = async(symbolData, server) => {

    const blockchainDataPromises = symbolData.check.map(async (contract) => {
        const data = await screener.getDexSymbolByContract(contract.contract, contract.network, server.ip);
        return data ? { contract, data } : null;
    });

    const blockchainDataResults = (await Promise.all(blockchainDataPromises)).filter(Boolean);
    return blockchainDataResults;
}


const startProcess = async () => {
    try {
        while (true) {
            
            const symbolsData = await getSymbolsArr()

            const parallelTasksLimit = 1

            for (let i = 0; i < symbolsData.length; i += parallelTasksLimit) {

                await Promise.all(symbolsData.slice(i, i + parallelTasksLimit).map(async (symbol) => {
                    await processArbitrage(symbol)
                }))
                await delay(1000);
            }
        }

    } catch (error) {
        console.error('Error in startProcess:', error);
        await delay(10000);
        await startProcess(); 
    }
};

const delay = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

startProcess().catch(err => {
    console.error('Failed to start process:', err);
});


module.exports = { event }
