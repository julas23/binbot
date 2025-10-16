
// version -------------------------------------------------------------------------------------------------------------------------
const version = "v0.8.4";
const time0 = Math.floor(Date.now() / 1000);
var time = time0;
// ---------------------------------------------------------------------------------------------------------------------------------
// REQUIREMENTS --------------------------------------------------------------------------------------------------------------------

    import fs from 'fs';
    import chalk from 'chalk';
    import Binance from 'binance';
    const { MainClient } = Binance;
    const { USDMClient } = Binance;
    const { WebsocketClient } = Binance;
    import WebSocket from 'ws';


// ---------------------------------------------------------------------------------------------------------------------------------
// Config --------------------------------------------------------------------------------------------------------------------------

    let config;
    try{
        config = JSON.parse(fs.readFileSync("./config.json",'utf8'));

    } catch(err) {
        throw Error(chalk.red('Could NOT parse config file!\nPlease double check it!...'));
    }
    
    let keys;
    try{
        keys = fs.readFileSync("./keys.txt",'utf8').split('\n');

    } catch(err) {
        throw Error(chalk.red('Could NOT parse keys file!\nPlease double check it!...'));
    }

    if(!config.USETESTNET && (keys[0] == '' || keys[1] == ''))throw Error(chalk.red('Could NOT FIND a MAINNET key inside the "keys.txt" file!\nPlease double check it!...'));
    else if(!config.USETESTNET && keys[0].length < 64)throw Error(chalk.red('MAINNET key is INVALID!\nPlease double check it!...'));
    else if(!config.USETESTNET && keys[1].length < 64)throw Error(chalk.red('MAINNET secret is INVALID!\nPlease double check it!...'));
    if(!config.USETESTNET && keys[0].length > 64){
        console.log(chalk.yellow('\nMAINNET key is LONGER than expected!\nWILL TRY TO TRIM IT, but please CHECK IT!!!...\n'));
        keys[0] = keys[0].trim();
    }
    if(!config.USETESTNET && keys[1].length > 64){
        console.log(chalk.yellow('\nMAINNET secret is LONGER than expected!\nWILL TRY TO TRIM IT, but please CHECK IT!!!...\n'));
        keys[1] = keys[1].trim();
    }

    if(config.USETESTNET && (keys[2] == '' || keys[3] == ''))throw Error(chalk.red('Could NOT FIND a TESTNET key inside the "keys.txt" file!\nPlease double check it!...'));
    else if(config.USETESTNET && keys[2].length < 64)throw Error(chalk.red('TESTNET key is INVALID!\nPlease double check it!...'));
    else if(config.USETESTNET && keys[3].length < 64)throw Error(chalk.red('TESTNET secret is INVALID!\nPlease double check it!...'));
    if(config.USETESTNET && keys[2].length > 64){
        console.log(chalk.yellow('\nTESTNET key is LONGER than expected!\nWILL TRY TO TRIM IT, but please CHECK IT!!!...\n'));
        keys[2] = keys[2].trim();
    }
    if(config.USETESTNET && keys[3].length > 64){
        console.log(chalk.yellow('\nTESTNET secret is LONGER than expected!\nWILL TRY TO TRIM IT, but please CHECK IT!!!...\n'));
        keys[3] = keys[3].trim();
    }

// ---------------------------------------------------------------------------------------------------------------------------------
// Constants -----------------------------------------------------------------------------------------------------------------------

    const TEMPORARY_KEYS = {};
        TEMPORARY_KEYS.key = config.USETESTNET?
            keys[2]:
            keys[0];
        TEMPORARY_KEYS.secret = config.USETESTNET?
            keys[3]:
            keys[1];

    const mainClient = new MainClient({
        api_key: TEMPORARY_KEYS.key,
        api_secret: TEMPORARY_KEYS.secret,
    },{},config.USETESTNET);
    const usdmClient = new USDMClient({
        api_key: TEMPORARY_KEYS.key,
        api_secret: TEMPORARY_KEYS.secret,
        disableTimeSync: false
    },{},config.USETESTNET);
    const wsClient = new WebsocketClient({
        api_key: TEMPORARY_KEYS.key,
        api_secret: TEMPORARY_KEYS.secret,
    });
    const ws = Array(config.MARKETS.length);
    //for(var m=0;m<config.MARKETS.length;m++)ws[m] = new WebSocket('ws://localhost:'+(10000+m));

    const DATA = {};
        DATA.marketsList = [];
        DATA.markets = {};
        DATA.threads = null;
        DATA.profit = 0;
        DATA.tickers = null;
        DATA.feeTier = null;
        DATA.balance = 0;
        DATA.ini_balance = 0;
        DATA.av_balance = 0;
        DATA.fpnl = 0;
        DATA.totalPositions = [0,0];
        DATA.hitt_miss = [0,0];
        DATA.perPosition = [0,0];
        DATA.oID = + new Date();
        DATA.grids = 1;
        DATA.getReport = null;
        DATA.getTotalProfit = null;
        DATA.reportData = [];
        DATA.POSITIONPERCENTAGE = 0;
        DATA.LASTCLOSE = null;
        DATA.cycle = [0,0];
// ---------------------------------------------------------------------------------------------------------------------------------
// Variables -----------------------------------------------------------------------------------------------------------------------

    

// ---------------------------------------------------------------------------------------------------------------------------------
// Functions -----------------------------------------------------------------------------------------------------------------------

// ----------------------------------- wsClient:

    wsClient.on('message', async function(data){
        //if(config.DEBUG)console.log('raw message received ', JSON.stringify(data, null, 2) );
        if(data.e == "aggTrade")BINANCE.updateTrades(data);
        else if(data.e == "depthUpdate")BINANCE.updateSubsBook(data);
        else if(data.e == "ACCOUNT_UPDATE"){
            //console.log(data);
            if(data.a && data.a.P && data.a.P.length){
                const entry_price_sums = {};
                const position_amount_sum = {};

                for(var m=0;m<data.a.P.length;m++){
                    const market = data.a.P[m].s;
                    if(!entry_price_sums[market])entry_price_sums[market] = 0;
                    if(!position_amount_sum[market])position_amount_sum[market] = 0;
                    entry_price_sums[market] += (+data.a.P[m].ep);
                    position_amount_sum[market] += (+data.a.P[m].pa);
                }

                const markets = Object.keys(entry_price_sums);

                for(var M=0;M<markets.length;M++){
                    const single_pos = DATA.markets[markets[M]].getQty(entry_price_sums[markets[M]]);
                    const order = Math.round(position_amount_sum[markets[M]]/single_pos);
                    DATA.markets[markets[M]].order = order;
                    DATA.reportData.push("Market: " + markets[M] + ", is now at order: [" + chalk.yellow(order) + "] with ("+chalk.yellow(position_amount_sum[markets[M]])+")");
                }
            }
            const res = await usdmClient.getBalance().catch((err) => {
                if(config.DEBUG)console.log(err.message);
            });

            if(res != undefined)for(var b=0;b<res.length;b++)if(res[b].asset == config.QUOTE){
                DATA.balance = +res[b].balance;
                DATA.av_balance = +res[b].availableBalance;
                DATA.fpnl = +res[b].crossUnPnl|0;

                TOOLS.getTotalProfit();

                if(DATA.ini_balance == 0)DATA.ini_balance = DATA.balance;

                DATA.reportData.push("IniBal: " + DATA.ini_balance + " "+config.QUOTE+"| Bal: " + DATA.balance + " "+config.QUOTE+"| Profit: " + DATA.profit + " %");
                break;
            }
        } else if(data.e == "markPriceUpdate"){
            const market = data.s;
            if(DATA.markets[market].index[1] == null)DATA.markets[market].index[1] = +data.i;
            else {
                DATA.markets[market].index[1] = DATA.markets[market].index[0];
                DATA.markets[market].index[0] = +data.i;
            }
            if(DATA.markets[market].mark[1] == null)DATA.markets[market].mark[1] = +data.p;
            else {
                DATA.markets[market].mark[1] = DATA.markets[market].mark[0];
                DATA.markets[market].mark[0] = +data.p;
            }
            //if(DATA.markets[market].started)BINANCE.decide(market);
        } else if(data.e == "ORDER_TRADE_UPDATE")BINANCE.handleOrderUpdate(data.o);
        //else if(config.DEBUG)console.log('raw message received ', JSON.stringify(data, null, 2) );
        //console.log(data);
    });

    wsClient.on('open', (data) => {
        //if(config.DEBUG)console.log('connection opened open: ', data.wsKey, data.ws.target.url );
    });

    wsClient.on('reply', (data) => {
        //if(config.DEBUG)console.log('log reply: ', JSON.stringify(data, null, 2) );
    });

    // receive notification when a ws connection is reconnecting automatically
    wsClient.on('reconnecting', (data) => {
        //if(config.DEBUG)console.log('ws automatically reconnecting.... ', data?.wsKey );
    });

    // receive notification that a reconnection completed successfully (e.g use REST to check for missing data)
    wsClient.on('reconnected', (data) => {
        //if(config.DEBUG)console.log('ws has reconnected ', data?.wsKey );
    });

    // Recommended: receive error events (e.g. first reconnection failed)
    wsClient.on('error', (data) => {
        //if(config.DEBUG)console.log('ws saw error ', data?.wsKey );
    });

// ----------------------------------- TOOLS:

    var TOOLS = {};
        TOOLS.time = 0;
        TOOLS.setTime = async function(){
            TOOLS.time++;
            time = time0+TOOLS.time;
            const markets = Object.keys(DATA.markets);
            for(var m=0;m<markets.length;m++){
                
                if(DATA.markets[markets[m]].price > 0 && !DATA.markets[markets[m]].started && DATA.markets[markets[m]].prices[config.PRICETICKS-1] != undefined){
                    DATA.markets[markets[m]].started = true;
                    DATA.markets[markets[m]].setGrid(markets[m]);
                } else if(DATA.markets[markets[m]].prices[config.PRICETICKS-1] == undefined){
                    const left = config.PRICETICKS - DATA.markets[markets[m]].prices.length;
                    if(config.VERBOSE)DATA.reportData.push("Getting Data: " + chalk.yellow(left) + " TICKS left on market [" + chalk.magenta(markets[m]) + "]");
                } 
            }
        };
        TOOLS.getTime = function calcTime(sec) {
            var pad = function(num, size) { return ('000' + num).slice(size * -1); },
            time = parseFloat(sec).toFixed(3),
            days = Math.floor(time / 60 / 60 / 24),
            hours = Math.floor(time / 60 / 60) % 24,
            minutes = Math.floor(time / 60) % 60,
            seconds = Math.floor(time - minutes * 60);

            return pad(days, 3) + 'd:' + pad(hours, 2) + 'h:' + pad(minutes, 2) + 'm:' + pad(seconds, 2) + 's';
        };
        TOOLS.avg = function(inp){
            var out = 0;
            var cnt = 0;
            for(var i=0;i<inp.length;i++)if(inp[i] != undefined){out += inp[i];cnt++;};
            out = out/cnt;
            return out;
        };
        TOOLS.getPos = function(bal, lev, per, pr, tk){
            var temp = 1 / (+tk);
            return Math.ceil((bal*per*lev/pr)*temp)/temp;
        };
        TOOLS.roundTo = function(n, to){
            var temp = 1 / (+to);
            return Math.round(n * temp) / temp; 
        };
        TOOLS.floorTo = function(n, to){
            var temp = 1 / (+to);
            return Math.floor(n * temp) / temp; 
        };
        TOOLS.ceilTo = function(n, to){
            var temp = 1 / (+to);
            return Math.ceil(n * temp) / temp; 
        };
        TOOLS.avgM = function(inp){
            var out = 0;
            var cnt = 0;
            for(var i=0;i<inp.length;i++)if(inp[i] != undefined){out += inp[i]>0?inp[i]:-inp[i];cnt++;};
            out = out/cnt;
            return out;
        };
        TOOLS.wAvg = function(inp){
            var out = 0;
            var cnt = 0;
            var totW = 0;
            for(var i=0;i<inp.length;i++)if(inp[i][1] != undefined){totW += inp[i][1];cnt++;};
            for(var i=0;i<inp.length;i++)if(inp[i][1] != undefined)out += inp[i][0]*(inp[i][1]/totW);
            return out;
        };
        TOOLS.getTotalProfit = function(){
            DATA.profit = +((DATA.balance/DATA.ini_balance-1)*100).toFixed(config.DECIMALS);
            if(isNaN(DATA.profit) || DATA.profit == Infinity)DATA.profit = 0;
        };
        TOOLS.log = function(){
            if(DATA.reportData.length > 0)for(var l=0;l<DATA.reportData.length;l++){
                TOOLS.getTotalProfit();
                if(typeof DATA.reportData[l] == "Object")DATA.reportData[l] = JSON.stringify(DATA.reportData[l]);
                console.log(DATA.reportData[l]);
                fs.appendFileSync(config.LOGFILE,DATA.reportData[l],'utf8');
            }
            DATA.reportData = [];
        }
        TOOLS.getReport = function(){

            DATA.reportData = DATA.reportData.slice(-config.REPORTLINES);

            TOOLS.getTotalProfit();
            let color;
            if(DATA.profit == 0)color = "white";
            else if(DATA.profit > 0)color = "green";
            else if(DATA.profit < 0)color = "red";
            let color1;
            if(DATA.av_balance == (1-config.MAXRISK*3)*DATA.balance)color1 = "white";
            else if(DATA.av_balance > (1-config.MAXRISK*3)*DATA.balance)color1 = "green";
            else if(DATA.av_balance < (1-config.MAXRISK*3)*DATA.balance)color1 = "red";
            let color2;
            if(DATA.fpnl == 0)color2 = "white";
            else if(DATA.fpnl > 0)color2 = "green";
            else if(DATA.fpnl < 0)color2 = "red";

            const sep0 = ' 🎄'+chalk.white('-----------------------------------------------------')+'🎄\n';
            const sep1 = ' |'+  chalk.red('+-----------------------------------------------------+')+'|\n';
            
            const user = "\tUSER ACCOUNT: " + config.USER + " on " + (config.USETESTNET?chalk.green('TESTNET'):chalk.red('MAINNET')) + "\n";
            const text0 = ' ' + ("Balance("+config.QUOTE+") ["+chalk.yellow(DATA.ini_balance.toFixed(3))+"]["+chalk[color](DATA.balance.toFixed(3))+"]["+chalk[color1](DATA.av_balance.toFixed(3))+"]["+chalk[color2](DATA.fpnl.toFixed(3))+"]\n");
            const text1 = ' ' + ("Profit ["+chalk[color]((DATA.balance-DATA.ini_balance).toFixed(8))+' '+config.QUOTE+" (" + chalk[color](DATA.profit.toFixed(8)) + " %)] | Cycles: ["+DATA.cycle[0]+'|'+DATA.cycle[1]+"]\n");
            const config0 = ' ' + ("CONFIG1: ["+chalk.cyan(config.CLOSEALLPOSITIONS)+"]["+chalk.cyan(config.VERBOSE)+"]["+chalk.cyan(config.DEBUG)+"]["+chalk.cyan(config.PLACEORDERS)+"]["+chalk.cyan(config.USEMAXLEVERAGE)+"]["+chalk.cyan(config.MARGIN)+"]["+chalk.cyan(config.QUOTE)+"]\n");
            const config1 = ' ' + ("CONFIG2: ["+chalk.cyan(config.REPORTLINES)+"]["+chalk.cyan(config.REPORTCOLS)+"]["+chalk.cyan(config.MARKETSDEPTH)+"]["+chalk.cyan(config.MARKETSUPDATE)+"]["+chalk.cyan(config.REFRESHTIME)+"]["+chalk.cyan(config.PRICETICKS)+"]\n");
            const config2 = ' ' + ("CONFIG3: ["+chalk.cyan(config.DECIMALS)+"]["+chalk.cyan(config.MAXORDERS)+"]["+chalk.cyan(config.MAXRISK)+"]["+chalk.cyan(config.MINPOSITION)+"]["+chalk.cyan(config.MINPROFIT)+"]["+chalk.cyan(config.MAXLOSS)+"]["+chalk.cyan(config.LEVERAGE)+"]\n");
            const data = '\n'+DATA.reportData.join('\n')+'\n\n';
            const sig = chalk.white(' |') + chalk.red('|') + version.padEnd(10, ' ') + chalk.red('|') + ("Merry Christmas, a 🎁 from: "+chalk.red("🦄 failed99 🦄")) + chalk.red('|') + chalk.white('|\n');
            const time1 = chalk.white(' |') + chalk.red('|') + (/*"Start: " + time0.getDay() +'/'+ time0.getMonth() +'/'+ time0.getFullYear() +' '+ time0.getHours() +':'+ time0.getMinutes() +':'+ time0.getSeconds() +*/"Running: "+TOOLS.getTime(TOOLS.time)).padStart(53, ' ') + chalk.red('|') + chalk.white('|\n');;

            console.clear();
            console.log(user+sep0+sep1+text0+text1+config0+config1+config2+sep1+sep0+data+sep0+sep1+time1+sep1+sig+sep1+sep0);
        };
        TOOLS.getPrecision = function(n){
            return n.toFixed(12).split('.')[1].includes('1')?1+n.toFixed(12).split('.')[1].lastIndexOf('1'):n.toFixed(12).split('.')[1].indexOf('0');
        }

// ----------------------------------- BINANCE:
    var BINANCE = {};
        BINANCE.buffTime = 100;
        BINANCE.buffer = [];
        BINANCE.createMarket = function(mkt){
            DATA.markets[mkt] = {};
            DATA.markets[mkt].started = false;
            DATA.markets[mkt].buy = function(pr, ps, rd, cl, tp, mk){
                
                DATA.markets[mk].lastLong = ('x-15PC4ZJy'+(++DATA.oID));
                DATA.markets[mk].longTrigger = +((+pr)-(+pr)*config.TAKEPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));

                    if(config.VERBOSE)DATA.reportData.push(chalk.green("BUY") + " on Market ["+chalk.magenta(mk)+"] @ ["+chalk.blue(pr)+"] with ["+chalk.magenta(ps)+"]");
                    //if(config.VERBOSE)DATA.reportData.push("R-In: " + chalk.yellow(DATA.markets[mk].rangeIn.toFixed(config.DECIMALS)));

                    var rec = {};
                    rec.method = "submitNewOrder";
                    rec.object = {
                        newClientOrderId : DATA.markets[mk].lastLong,
                        symbol : mk,
                        side : "BUY",
                        type : tp
                    };

                    if(tp == "LIMIT")rec.object.timeInForce = "GTC";
                    if(tp == "LIMIT")rec.object.price = pr;
                    // EXPERIMENTAL:                    
                    if(rd)rec.object.reduceOnly = true;
                    if(cl){
                        rec.object.closePosition = true;
                        rec.object.stopPrice = pr;
                    } else rec.object.quantity = ps;

                    if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
                    //else console.log(rec);

                
            };
            DATA.markets[mkt].sell = function(pr, ps, rd, cl, tp, mk){
                
                DATA.markets[mk].lastShort = ('x-15PC4ZJy'+(++DATA.oID));
                DATA.markets[mk].shortTrigger = +((+pr)+(+pr)*config.TAKEPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));

                    if(config.VERBOSE)DATA.reportData.push(chalk.red("SELL") + " on Market ["+chalk.magenta(mk)+"] @ ["+chalk.blue(pr)+"] with ["+chalk.magenta(ps)+"]");
                    //if(config.VERBOSE)DATA.reportData.push("R-In: " + chalk.yellow(DATA.markets[mk].rangeIn.toFixed(config.DECIMALS)));

                    var rec = {};
                    rec.method = "submitNewOrder";
                    rec.object = {
                        newClientOrderId : DATA.markets[mk].lastShort,
                        symbol : mk,
                        side : "SELL",
                        type : tp
                    };

                    if(tp == "LIMIT")rec.object.timeInForce = "GTC";
                    if(tp == "LIMIT")rec.object.price = pr;
                    // EXPERIMENTAL:
                    if(rd)rec.object.reduceOnly = true;
                    if(cl){
                        rec.object.closePosition = true;
                        rec.object.stopPrice = pr;
                    } else rec.object.quantity = ps;

                    if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
                    //else console.log(rec);
                
            };
            DATA.markets[mkt].order = 0;
            DATA.markets[mkt].index = [null,null];
            DATA.markets[mkt].mark = [null,null];
            DATA.markets[mkt].price = null;
            DATA.markets[mkt].prices = [];
            DATA.markets[mkt].trades = [];
            DATA.markets[mkt].vel = null;
            DATA.markets[mkt].mom = null;
            DATA.markets[mkt].target = null;
            DATA.markets[mkt].candles = Array(config.CANDLES);
            DATA.markets[mkt].currentTop = null;
            DATA.markets[mkt].currentBot = null;
            DATA.markets[mkt].lastTrail = null;
            DATA.markets[mkt].inLong = false;
            DATA.markets[mkt].lastLong = null;
            DATA.markets[mkt].longTrigger = null;
            DATA.markets[mkt].inShort = false;
            DATA.markets[mkt].lastShort = null;
            DATA.markets[mkt].shortTrigger = null;
            DATA.markets[mkt].position = null;
            DATA.markets[mkt].acumulated = null;
            DATA.markets[mkt].asks = [];
            DATA.markets[mkt].bids = [];
            DATA.markets[mkt].rangeIn = null;
            DATA.markets[mkt].rangeOut = null;
            DATA.markets[mkt].pal = [0,1];
            DATA.markets[mkt].fee = null;
            DATA.markets[mkt].lev = null;
            DATA.markets[mkt].vol = null;
            DATA.markets[mkt].grid = [];
            DATA.markets[mkt].gridSteps = null;
            DATA.markets[mkt].setGrid = function(mk){

                //DATA.markets[mk].rangeIn = DATA.markets[mk].order>=0?config.MINPROFIT*(DATA.markets[mk].order+1):config.MINPROFIT*((-DATA.markets[mk].order)+1);

                var b_pr = DATA.markets[mk].getPr("BUY");
                const b_qty = DATA.markets[mk].getQty(+b_pr);
                var s_pr = DATA.markets[mk].getPr("SELL");
                const s_qty = DATA.markets[mk].getQty(+s_pr);

                //const r = ((+s_pr)-(+b_pr))/2;
                //b_pr = DATA.markets[mk].order?(DATA.markets[mk].order>0?b_pr-(DATA.markets[mk].order+1)*r:b_pr-(-DATA.markets[mk].order+1)*r):b_pr;
                //b_pr = b_pr.toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));
                //s_pr = DATA.markets[mk].order?(DATA.markets[mk].order>0?s_pr+(DATA.markets[mk].order+1)*r:s_pr+(-DATA.markets[mk].order+1)*r):s_pr;
                //s_pr = s_pr.toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));

                if(+s_pr > 0 && +b_pr > 0){
                    if(DATA.markets[mk].order == 0)DATA.markets[mk].position = DATA.markets[mk].acumulated = +DATA.markets[mk].getQty(((+b_pr)+(+s_pr))/2);
                    //else DATA.markets[mk].acumulated += DATA.markets[mk].position;
                    //else if(DATA.markets[mk].order > 0)DATA.markets[mk].acumulated += (DATA.markets[mk].order+1) * DATA.markets[mk].position;
                    //else if(DATA.markets[mk].order < 0)DATA.markets[mk].acumulated += ((-DATA.markets[mk].order)+1) * DATA.markets[mk].position;
                    
                    if(DATA.markets[mk].order > -config.MAXORDERS){
                        DATA.reportData.push(chalk.red("SHORT ") + "OPEN POSITION on Market: [" + chalk.magenta(mk) + "] @ [" + chalk.blue(s_pr) + "]");
                        DATA.markets[mk].sell(s_pr, DATA.markets[mk].acumulated, false, false, "LIMIT", mk);
                    } else {
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("REATCHED MAXORDERS LIMIT of ["+config.MAXORDERS+"]!..."));
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("Will NOT place more orders on ["+mk+"]..."));
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("Only close the ones already open..."));
                    }
                    if(DATA.markets[mk].order < config.MAXORDERS){
                        DATA.reportData.push(chalk.green("LONG ") + "OPEN POSITION on Market: [" + chalk.magenta(mk) + "] @ [" + chalk.blue(b_pr) + "]");
                        DATA.markets[mk].buy(b_pr, DATA.markets[mk].acumulated, false, false, "LIMIT", mk); 
                    } else {
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("REATCHED MAXORDERS LIMIT of ["+config.MAXORDERS+"]!..."));
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("Will NOT place more orders on ["+mk+"]..."));
                        if(config.DEBUG)DATA.reportData.push(chalk.yellow("Only close the ones already open..."));
                    }
                }
            };
            DATA.markets[mkt].closePosition = function(mk , sd){

                const b_pr = DATA.markets[mk].getBuyPr();
                const b_qty = DATA.markets[mk].getQty(+b_pr);
                const s_pr = DATA.markets[mk].getSellPr();
                const s_qty = DATA.markets[mk].getQty(+s_pr);

                DATA.markets[mk].grid[0] = s_pr;
                DATA.markets[mk].grid[1] = b_pr;

                if(+s_pr > 0 && +b_pr > 0){
                    if(DATA.markets[mk].order == 0)DATA.markets[mk].position = DATA.markets[mk].acumulated = +DATA.markets[mk].getQty(((+b_pr)+(+s_pr))/2);
                    else if(DATA.markets[mk].order > 0)DATA.markets[mk].acumulated = DATA.markets[mk].order * DATA.markets[mk].position;
                    else if(DATA.markets[mk].order < 0)DATA.markets[mk].acumulated = (-DATA.markets[mk].order) * DATA.markets[mk].position;
                    
                    //if(DATA.markets[mk].order == 0)DATA.markets[mk].position = DATA.markets[mk].position.toFixed(DATA.markets[mk].quantityPrecision);
                    //else DATA.markets[mk].acumulated = DATA.markets[mk].acumulated.toFixed(DATA.markets[mk].quantityPrecision);

                    if(DATA.markets[mk].order > -config.MAXORDERS && sd == "LONG"){
                        DATA.reportData.push(chalk.cyan("LONG ") + chalk.red("STOP LOSS") + " on Market: [" + chalk.magenta(mk) + "] @ [" + chalk.blue(s_pr) + "]");
                        DATA.markets[mk].sell(false, DATA.markets[mk].inLong?DATA.markets[mk].acumulated:DATA.markets[mk].position, true , false, "MARKET", mk);
                    } 
                    if(DATA.markets[mk].order < config.MAXORDERS && sd == "SHORT"){
                        DATA.reportData.push(chalk.cyan("SHORT ") + chalk.red("STOP LOSS") + " on Market: [" + chalk.magenta(mk) + "] @ [" + chalk.blue(b_pr) + "]");
                        DATA.markets[mk].buy(false, DATA.markets[mk].inShort?DATA.markets[mk].acumulated:DATA.markets[mk].position, true , false, "MARKET", mk); 
                    } 
                }
            };
            DATA.markets[mkt].modifyOrder = function(pr, ps, sd, mk){
            if(config.VERBOSE)DATA.reportData.push(chalk.cyan(sd) + ' ' + chalk.red("INVERT MARKET") + " on Market ["+chalk.magenta(mk)+"] will be @ ["+chalk.blue(pr)+"] ("+sd+'>'+(sd=="BUY"?"SELL":"BUY")+")");
                 
                    var rec = {};
                    rec.method = "modifyOrder";
                    rec.object = {
                        origClientOrderId : (sd=="BUY")?DATA.markets[mkt].lastLong:DATA.markets[mkt].lastShort,
                        symbol : mk,
                        side : sd,
                        quantity : ps,
                        price : pr
                    };

                if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
            };
            DATA.markets[mkt].stopMarket = function(mk , sd, pr){
            if(config.VERBOSE)DATA.reportData.push(chalk.cyan(sd) + ' ' + chalk.red("STOP MARKET") + " on Market ["+chalk.magenta(mk)+"] will be @ ["+chalk.blue(pr)+"]");
                 

                    var rec = {};
                    rec.method = "submitNewOrder";
                    rec.object = {
                        newClientOrderId : ('x-15PC4ZJy'+(++DATA.oID)),
                        symbol : mk,
                        side : sd,
                        type : "STOP_MARKET",
                        stopPrice : pr,
                        closePosition : true
                    };

                if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
            };
            DATA.markets[mkt].takeMarket = function(mk , sd, pr){
                if(config.VERBOSE)DATA.reportData.push(chalk.cyan(sd) + ' ' + chalk.green("TAKE PROFIT MARKET") + " on Market ["+chalk.magenta(mk)+"] will be @ ["+chalk.blue(pr)+"]");


                    var rec = {};
                    rec.method = "submitNewOrder";
                    rec.object = {
                        newClientOrderId : ('x-15PC4ZJy'+(++DATA.oID)),
                        symbol : mk,
                        side : sd,    
                        type : "TAKE_PROFIT_MARKET",
                        stopPrice : pr,
                        closePosition : true
                    };

                if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
            };
	        DATA.markets[mkt].getQty = function(pr){
                if(DATA.balance*DATA.POSITIONPERCENTAGE > config.MINPOSITION)return +(DATA.balance*DATA.POSITIONPERCENTAGE/pr).toFixed(DATA.markets[mkt].quantityPrecision);
                else return +(config.MINPOSITION/pr).toFixed(DATA.markets[mkt].quantityPrecision);
            };
            DATA.markets[mkt].getPr = function(sd){
                const inv = (DATA.markets[mkt].order<0)?-1:1;
                var incr = 0;
                const price = +DATA.markets[mkt].price;
                if(inv*DATA.markets[mkt].order>1){
                    for(var p=0;p<inv*DATA.markets[mkt].order;p++){
                        incr += DATA.markets[mkt].pal[0]+DATA.markets[mkt].pal[1];
                        DATA.markets[mkt].pal[0] = DATA.markets[mkt].pal[1];
                        DATA.markets[mkt].pal[1] = incr;
                    }
                    DATA.markets[mkt].pal[0] = 0;
                    DATA.markets[mkt].pal[1] = 1; 
                    incr = incr*price*config.MINPROFIT;
                } else if(inv*DATA.markets[mkt].order==1){
                    incr=2;
                    incr=incr*price*config.MINPROFIT;
                } else if(inv*DATA.markets[mkt].order==0){
                    incr=1;
                    incr=incr*price*config.MINPROFIT;
                }
                return ((sd == "SELL")?price+incr:price-incr).toFixed(TOOLS.getPrecision(DATA.markets[mkt].tickSize));
            };
        };
        BINANCE.sendBuffer = async function(){
            const len = BINANCE.buffer.length;
            if(len > 0 && BINANCE.buffer[0].method != undefined){

                const market = BINANCE.buffer[0].object.symbol;
                const side = BINANCE.buffer[0].object.side;
                const reduceOnly = !!BINANCE.buffer[0].object.reduceOnly;

                const res = await usdmClient[BINANCE.buffer[0].method](BINANCE.buffer[0].object).catch(error => { 
                    DATA.reportData.push('ERROR: ' + error.message); 
                });
                //DATA.markets[market].setGrid(market);
                //console.log(res);

                if(res != undefined){
                    if(config.DEBUG)DATA.reportData.push("REQUEST WAS " + chalk.green("ACCEPTED!") + " on " + chalk.magenta(market));
                    if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                    else BINANCE.buffer = [];
                    setTimeout(BINANCE.sendBuffer,BINANCE.buffTime);
                } else {

                    if(res != undefined && res.code != undefined) {
                        if(res.code == -1000){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNKNOWN"), "An unknown error occurred while processing the request.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1001){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("DISCONNECTED"), "Internal error; unable to process your request. Please try again.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1002){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNAUTHORIZED"), "You are not authorized to execute this request.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1003){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TOO_MANY_REQUESTS"), "Too many requests queued.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1004){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("DUPLICATE_IP"), "This IP is already on the white list.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1005){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_SUCH_IP"), "No such IP has been white listed.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1006){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNEXPECTED_RESP"), "An unexpected response was received from the message bus. Execution status unknown.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1007){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TIMEOUT"), "Timeout waiting for response from backend server. Send status unknown; execution status unknown.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1010){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ERROR_MSG_RECEIVED"), "ERROR_MSG_RECEIVED.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1011){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NON_WHITE_LIST"), "This IP cannot access this route.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1013){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_MESSAGE"), "INVALID_MESSAGE.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1014){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNKNOWN_ORDER_COMPOSITION"), "Unsupported order combination.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1015){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TOO_MANY_ORDERS"), "Too many new orders.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1016){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("SERVICE_SHUTTING_DOWN"), "This service is no longer available.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1020){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNSUPPORTED_OPERATION"), "This operation is not supported.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1021){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_TIMESTAMP"), "Timestamp for this request is outside of the recvWindow.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1022){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_SIGNATURE"), "Signature for this request is not valid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1023){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("START_TIME_GREATER_THAN_END_TIME"), "Start time is greater than end time.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1099){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NOT_FOUND"), "Not found, unauthenticated, or unauthorized.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1100){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ILLEGAL_CHARS"), "Illegal characters found in a parameter.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1101){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TOO_MANY_PARAMETERS"), "Too many parameters sent for this endpoint.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1102){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MANDATORY_PARAM_EMPTY_OR_MALFORMED"), "A mandatory parameter was not sent, was empty/null, or malformed.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1103){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNKNOWN_PARAM"), "An unknown parameter was sent.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1104){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNREAD_PARAMETERS"), "Not all sent parameters were read.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1105){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PARAM_EMPTY"), "A parameter was empty.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1106){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PARAM_NOT_REQUIRED"), "A parameter was sent when not required.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1108){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_ASSET"), "Invalid asset.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1109){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_ACCOUNT"), "Invalid account.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1110){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_INSTRUMENT_TYPE"), "Invalid symbolType.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1111){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_PRECISION"), "Precision is over the maximum defined for this asset.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1112){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_DEPTH"), "No orders on book for symbol.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1113){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("WITHDRAW_NOT_NEGATIVE"), "Withdrawal amount must be negative.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1114){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TIF_NOT_REQUIRED"), "TimeInForce parameter sent when not required.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1115){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_TIF"), "Invalid timeInForce.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1116){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_ORDER_TYPE"), "Invalid orderType.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1117){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_SIDE"), "Invalid side.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1118){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("EMPTY_NEW_CL_ORD_ID"), "New client order ID was empty.");
                            BINANCE.buffer[0].object.newClientOrderId = ('x-15PC4ZJy'+(++DATA.oID));
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1119){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("EMPTY_ORG_CL_ORD_ID"), "Original client order ID was empty.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1120){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_INTERVAL"), "Invalid interval.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1121){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_SYMBOL"), "Invalid symbol.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1122){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_SYMBOL_STATUS"), "Invalid symbol status.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1125){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_LISTEN_KEY"), "This listenKey does not exist. Please use POST /fapi/v1/listenKey to recreate listenKey.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1126){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ASSET_NOT_SUPPORTED"), "This asset is not supported.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1127){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MORE_THAN_XX_HOURS"), "Lookup interval is too big.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1128){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONAL_PARAMS_BAD_COMBO"), "Combination of optional parameters invalid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1130){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_PARAMETER"), "Invalid data sent for a parameter.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -1136){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_NEW_ORDER_RESP_TYPE"), "Invalid newOrderRespType.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2010){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NEW_ORDER_REJECTED"), "NEW_ORDER_REJECTED.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2011){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("CANCEL_REJECTED"), "CANCEL_REJECTED.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2012){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("CANCEL_ALL_FAIL"), "Batch cancel failure.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2013){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_SUCH_ORDER"), "Order does not exist.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2014){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BAD_API_KEY_FMT"), "API-key format invalid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2015){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REJECTED_MBX_KEY"), "Invalid API-key, IP, or permissions for action.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2016){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_TRADING_WINDOW"), "No trading window could be found for the symbol. Try ticker/24hrs instead.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2017){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("API_KEYS_LOCKED"), "API Keys are locked on this account.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2018){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BALANCE_NOT_SUFFICIENT"), "Balance is insufficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2019){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MARGIN_NOT_SUFFICIEN"), "Margin is insufficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2020){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNABLE_TO_FILL"), "Unable to fill.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2021){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ORDER_WOULD_IMMEDIATELY_TRIGGER"), "Order would immediately trigger.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2022){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_REJECT"), "ReduceOnly Order is rejected.");
                            BINANCE.buffer[0].object.newClientOrderId = ('x-15PC4ZJy'+(++DATA.oID));
                            BINANCE.buffer[0].object.reduceOnly = false;
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2023){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("USER_IN_LIQUIDATION"), "User in liquidation mode now.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2024){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("POSITION_NOT_SUFFICIENT"), "Position is not sufficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2025){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_OPEN_ORDER_EXCEEDED"), "Reach max open order limit.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2026){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED"), "This OrderType is not supported when reduceOnly.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2027){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_LEVERAGE_RATIO"), "Exceeded the maximum allowable position at current leverage.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -2028){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MIN_LEVERAGE_RATIO"), "Leverage is smaller than permitted: insufficient margin balance.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4000){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_ORDER_STATUS"), "Invalid order status.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4001){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_LESS_THAN_ZERO"), "Price less than 0.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4002){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_GREATER_THAN_MAX_PRICE"), "Price greater than max price.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4003){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("QTY_LESS_THAN_ZERO"), "Quantity less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4004){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("QTY_LESS_THAN_MIN_QTY"), "Quantity less than min quantity.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4005){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("QTY_GREATER_THAN_MAX_QTY"), "Quantity greater than max quantity.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4006){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STOP_PRICE_LESS_THAN_ZERO"), "Stop price less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4007){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STOP_PRICE_GREATER_THAN_MAX_PRICE"), "Stop price greater than max price.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4008){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TICK_SIZE_LESS_THAN_ZERO"), "Tick size less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4009){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_PRICE_LESS_THAN_MIN_PRICE"), "Max price less than min price.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4010){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_QTY_LESS_THAN_MIN_QTY"), "Max qty less than min qty.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4011){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STEP_SIZE_LESS_THAN_ZERO"), "Step size less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4012){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_NUM_ORDERS_LESS_THAN_ZERO"), "Max mum orders less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4013){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_LESS_THAN_MIN_PRICE"), "Price less than min price.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4014){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_NOT_INCREASED_BY_TICK_SIZE"), "Price not increased by tick size.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4015){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_CL_ORD_ID_LEN"), "Client order id is not valid. Client order id length should not be more than 36 chars.");
                            BINANCE.buffer[0].object.newClientOrderId = ('x-15PC4ZJy'+(++DATA.oID));
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4016){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_HIGHTER_THAN_MULTIPLIER_UP"), "Price is higher than mark price multiplier cap.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4017){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MULTIPLIER_UP_LESS_THAN_ZERO"), "Multiplier up less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4018){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MULTIPLIER_DOWN_LESS_THAN_ZERO"), "Multiplier down less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4019){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("COMPOSITE_SCALE_OVERFLOW"), "Composite scale too large.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4020){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TARGET_STRATEGY_INVALID"), "Target strategy invalid for orderType '%s', reduceOnly '%b'.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4021){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_DEPTH_LIMIT"), "Invalid depth limit. '%s' is not valid depth limit.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4022){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("WRONG_MARKET_STATUS"), "Market status sent is not valid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4023){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("QTY_NOT_INCREASED_BY_STEP_SIZE"), "Qty not increased by step size.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4024){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_LOWER_THAN_MULTIPLIER_DOWN"), "Price is lower than mark price multiplier floor.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4025){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MULTIPLIER_DECIMAL_LESS_THAN_ZERO"), "Multiplier decimal less than zero.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4026){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("COMMISSION_INVALID"), "Commission invalid. %s less than zero. %s absolute value greater than %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4027){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_ACCOUNT_TYPE"), "Invalid account type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4028){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_LEVERAGE"), "Invalid leverage. Leverage %s is not valid. Leverage %s already exist with %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4029){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_TICK_SIZE_PRECISION"), "Tick size precision is invalid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4030){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_STEP_SIZE_PRECISION"), "Step size precision is invalid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4031){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_WORKING_TYPE"), "Invalid parameter working type. Invalid parameter working type: %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4032){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("EXCEED_MAX_CANCEL_ORDER_SIZE"), "Exceed maximum cancel order size. Invalid parameter working type: %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4033){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INSURANCE_ACCOUNT_NOT_FOUND"), "Insurance account not found.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4044){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_BALANCE_TYPE"), "Balance Type is invalid.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4045){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_STOP_ORDER_EXCEEDED"), "Reach max stop order limit.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4046){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_NEED_TO_CHANGE_MARGIN_TYPE"), "No need to change margin type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4047){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("THERE_EXISTS_OPEN_ORDERS"), "Margin type cannot be changed if there exists open orders.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4048){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("THERE_EXISTS_QUANTITY"), "Margin type cannot be changed if there exists position.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4049){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADD_ISOLATED_MARGIN_REJECT"), "Add margin only support for isolated position.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4050){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("CROSS_BALANCE_INSUFFICIENT"), "Cross balance insufficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4051){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ISOLATED_BALANCE_INSUFFICIENT"), "Isolated balance insufficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4052){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN"), "No need to change auto add margin.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4053){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("AUTO_ADD_CROSSED_MARGIN_REJECT"), "Auto add margin only support for isolated position.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4054){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADD_ISOLATED_MARGIN_NO_POSITION_REJECT"), "Cannot add position margin: position is 0.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4055){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("AMOUNT_MUST_BE_POSITIVE"), "Amount must be positive.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4056){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_API_KEY_TYPE"), "Invalid API key type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4057){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_RSA_PUBLIC_KEY"), "Invalid API public key.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4058){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MAX_PRICE_TOO_LARGE"), "Max price and priceDecimal too large, please check.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4059){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_NEED_TO_CHANGE_POSITION_SIDE"), "No need to change position side.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4060){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_POSITION_SIDE"), "Invalid position side.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4061){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("POSITION_SIDE_NOT_MATCH"), "Order's position side does not match user's setting.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4062){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_CONFLICT"), "Invalid or improper reduceOnly value.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4063){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_REQUEST_TYPE"), "Invalid options request type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4064){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_TIME_FRAME"), "Invalid options time frame.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4065){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_AMOUNT"), "Invalid options amount.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4066){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_EVENT_TYPE"), "Invalid options event type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4067){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS"), "Position side cannot be changed if there exists open orders.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4068){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("POSITION_SIDE_CHANGE_EXISTS_QUANTITY"), "Position side cannot be changed if there exists position.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4069){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_PREMIUM_FEE"), "Invalid options premium fee.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4070){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_CL_OPTIONS_ID_LEN"), "Client options id is not valid. Client options id length should be less than 32 chars.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4071){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_DIRECTION"), "Invalid options direction.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4072){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_PREMIUM_NOT_UPDATE"), "Premium fee is not updated, reject order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4073){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_PREMIUM_INPUT_LESS_THAN_ZERO"), "Input premium fee is less than 0, reject order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4074){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_AMOUNT_BIGGER_THAN_UPPER"), "Order amount is bigger than upper boundary or less than 0, reject order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4075){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_PREMIUM_OUTPUT_ZERO"), "Output premium fee is less than 0, reject order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4076){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_PREMIUM_TOO_DIFF"), "Original fee is too much higher than last fee.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4077){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_PREMIUM_REACH_LIMIT"), "Place order amount has reached to limit, reject order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4078){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_COMMON_ERROR"), "Options internal error.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4079){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPTIONS_ID"), "Invalid options id: %s. Duplicate options id %d for user %d.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4080){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_USER_NOT_FOUND"), "User not found with id: %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4081){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("OPTIONS_NOT_FOUND"), "Options not found with id: %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4082){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_BATCH_PLACE_ORDER_SIZE"), "Invalid number of batch place orders: %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4083){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PLACE_BATCH_ORDERS_FAIL"), "Fail to place batch orders.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4084){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UPCOMING_METHOD"), "Method is not allowed currently. Upcoming soon.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4085){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_NOTIONAL_LIMIT_COEF"), "Invalid notional limit coefficient.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4086){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_PRICE_SPREAD_THRESHOLD"), "Invalid price spread threshold.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4087){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_ORDER_PERMISSION"), "User can only place reduce only order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4088){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_PLACE_ORDER_PERMISSION"), "User cannot place order currently.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4104){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_CONTRACT_TYPE"), "Invalid contract type.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4114){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_CLIENT_TRAN_ID_LEN"), "Client tran id length should be less than 64 chars.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4115){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("DUPLICATED_CLIENT_TRAN_ID"), "Client tran id is duplicated. Client tran id should be unique within 7 days.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4118){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_MARGIN_CHECK_FAILED"), "ReduceOnly Order Failed. Please check your existing position and open orders.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4131){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MARKET_ORDER_REJECT"), "The counterparty's best price does not meet the PERCENT_PRICE filter limit.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4135){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_ACTIVATION_PRICE"), "Invalid activation price.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4137){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("QUANTITY_EXISTS_WITH_CLOSE_POSITION"), "Quantity must be zero with closePosition equals true.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4138){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("REDUCE_ONLY_MUST_BE_TRUE"), "Reduce only must be true with closePosition equals true.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4139){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ORDER_TYPE_CANNOT_BE_MKT"), "Order type cannot be market if it's unable to cancel.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4140){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_OPENING_POSITION_STATUS"), "Invalid symbol status for opening position.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4141){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("SYMBOL_ALREADY_CLOSED"), "Symbol is closed.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4142){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STRATEGY_INVALID_TRIGGER_PRICE"), "REJECT: take profit or stop order will be triggered immediately.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4144){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_PAIR"), "Invalid pair.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4161){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ISOLATED_LEVERAGE_REJECT_WITH_POSITION"), "Leverage reduction is not supported in Isolated Margin Mode with open positions.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4164){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MIN_NOTIONAL"), "Order's notional must be no smaller than 5.0 (unless you choose reduce only). Order's notional must be no smaller than %s (unless you choose reduce only).");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4165){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_TIME_INTERVAL"), "Invalid time interval. Maximum time interval is %s days.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4167){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ISOLATED_REJECT_WITH_JOINT_MARGIN"), "Unable to adjust to Multi-Assets mode with symbols of USDⓈ-M Futures under isolated-margin mode.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4168){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("JOINT_MARGIN_REJECT_WITH_ISOLATED"), "Unable to adjust to isolated-margin mode under the Multi-Assets mode.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4169){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("JOINT_MARGIN_REJECT_WITH_MB"), "Unable to adjust Multi-Assets Mode with insufficient margin balance in USDⓈ-M Futures.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4170){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("JOINT_MARGIN_REJECT_WITH_OPEN_ORDER"), "Unable to adjust Multi-Assets Mode with open orders in USDⓈ-M Futures.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4171){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("NO_NEED_TO_CHANGE_JOINT_MARGIN"), "Adjusted asset mode is currently set and does not need to be adjusted repeatedly.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4172){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("JOINT_MARGIN_REJECT_WITH_NEGATIVE_BALANCE"), "Unable to adjust Multi-Assets Mode with a negative wallet balance of margin available asset in USDⓈ-M Futures account.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4183){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ISOLATED_REJECT_WITH_JOINT_MARGIN"), "Price is higher than stop price multiplier cap. Limit price can't be higher than %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4184){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN"), "Price is lower than stop price multiplier floor. Limit price can't be lower than %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4192){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("COOLING_OFF_PERIOD"), "Trade forbidden due to Cooling-off Period.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4202){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_KYC_FAILED"), "Intermediate Personal Verification is required for adjusting leverage over 20x.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4203){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_ONE_MONTH_FAILED"), "More than 20x leverage is available one month after account registration.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4205){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_X_DAYS_FAILED"), "More than 20x leverage is available %s days after Futures account registration.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4206){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_KYC_LIMIT"), "Users in this country has limited adjust leverage. Users in your location/country can only access a maximum leverage of %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4208){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_ACCOUNT_SYMBOL_FAILED"), "Current symbol leverage cannot exceed 20 when using position limit adjustment service.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4209){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_SYMBOL_FAILED"), "The max leverage of Symbol is 20x. Leverage adjustment failed. Current symbol max leverage limit is %sx.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4210){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STOP_PRICE_HIGHER_THAN_PRICE_MULTIPLIER_LIMIT"), "Stop price is higher than price multiplier cap. Stop price can't be higher than %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4211){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("STOP_PRICE_LOWER_THAN_PRICE_MULTIPLIER_LIMIT"), "Stop price is lower than price multiplier floor. Stop price can't be lower than %s.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4400){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("TRADING_QUANTITATIVE_RULE"), "Futures Trading Quantitative Rules violated, only reduceOnly order is allowed, please try again later.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4401){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("COMPLIANCE_RESTRICTION"), "Compliance restricted account permission: can only place reduceOnly order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4402){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("COMPLIANCE_BLACK_SYMBOL_RESTRICTION"), "Dear user, as per our Terms of Use and compliance with local regulations, this feature is currently not available in your region.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -4403){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ADJUST_LEVERAGE_COMPLIANCE_FAILED"), "Dear user, as per our Terms of Use and compliance with local regulations, the leverage can only up to 10x in your region. Dear user, as per our Terms of Use and compliance with local regulations, the leverage can only up to %sx in your region.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5021){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("FOK_ORDER_REJECT"), "Due to the order could not be filled immediately, the FOK order has been rejected.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5022){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("GTX_ORDER_REJECT"), "Due to the order could not be executed as maker, the Post Only order will be rejected.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5024){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("MOVE_ORDER_NOT_ALLOWED_SYMBOL_REASON"), "Symbol is not in trading status. Order amendment is not permitted.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5025){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("LIMIT_ORDER_ONLY"), "Only limit order is supported.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5026){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("Exceed_Maximum_Modify_Order_Limit"), "Exceed maximum modify order limit.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5027){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("SAME_ORDER"), "No need to modify the order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5028){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("ME_RECVWINDOW_REJECT"), "Timestamp for this request is outside of the ME recvWindow.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5037){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_PRICE_MATCH"), "Invalid price match.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5038){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNSUPPORTED_ORDER_TYPE_PRICE_MATCH"), "Price match only supports order type: LIMIT, STOP AND TAKE_PROFIT.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5039){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("INVALID_SELF_TRADE_PREVENTION_MODE"), "Invalid self trade prevention mode.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5040){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("FUTURE_GOOD_TILL_DATE"), "The goodTillDate timestamp must be greater than the current time plus 600 seconds and smaller than 253402300799000 (UTC 9999-12-31 23:59:59).");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else if(res.code == -5041){
                            if(config.DEBUG) DATA.reportData.push(chalk.red("BBO_ORDER_REJECT"), "No depth matches this BBO order.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        } else {
                            if(config.DEBUG) DATA.reportData.push(chalk.red("UNKNOWN_ERROR"), "An unknown error occurred.");
                            setTimeout(BINANCE.sendBuffer, 1000);
                        }
                    } else if(BINANCE.buffer[0] && BINANCE.buffer[0].method == "submitNewOrder"){
			            if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                    	else BINANCE.buffer = [];
                    	setTimeout(BINANCE.sendBuffer,BINANCE.buffTime);
                    } else if(BINANCE.buffer[0] && BINANCE.buffer[0].method == "setLeverage"){
                        if(config.DEBUG)DATA.reportData.push("COULDN'T SET LEVERAGE for ["+chalk.magenta(market)+"]... IGNORING!...");
                        if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                        else BINANCE.buffer = [];
                        setTimeout(BINANCE.sendBuffer, 1000);
                    } else if(BINANCE.buffer[0] && BINANCE.buffer[0].method == "setMarginType"){
                        if(config.DEBUG)DATA.reportData.push("COULDN'T SET MARGIN for ["+chalk.magenta(market)+"]... IGNORING!...");
                        if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                        else BINANCE.buffer = [];
                        setTimeout(BINANCE.sendBuffer, 1000);
                    } else {
                        if(config.DEBUG)DATA.reportData.push("UNKNOWN ERROR on ["+chalk.magenta(market)+"]... WILL TRY TO IGNORE!...");
                        if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                        else BINANCE.buffer = [];
                        setTimeout(BINANCE.sendBuffer, 1000);
                    }
                    
                }
            } else if(len > 0 && BINANCE.buffer[0].method == undefined){
                if(BINANCE.buffer.length > 1)BINANCE.buffer = BINANCE.buffer.slice(1);
                else BINANCE.buffer = [];
                setTimeout(BINANCE.sendBuffer,BINANCE.buffTime);
            } else {
                setTimeout(BINANCE.sendBuffer,BINANCE.buffTime);
            }
        };
        BINANCE.getAccountInfo = async function(params){
            const cli = params.cli;
            let bal;
            let canTrade;
            let feeTier;
            let color;
            const res = await cli.getAccountInformation().catch(err => { console.log(err); });

            //console.log(res);

            canTrade = res.canTrade;
            if(!canTrade) throw new Error(chalk.red("Sorry, this user is FORBIDDEN to trade!...\n\nNothing can be done here...\n\nContact BINANCE on their Support Channel!"));

            feeTier = res.feeTier;
            for(var t=0;t<res.assets.length;t++){
                if(res.assets[t].asset == config.QUOTE){
                    DATA.balance = +res.assets[t].walletBalance;
                    DATA.av_balance = +res.assets[t].availableBalance;

                    if(DATA.ini_balance == 0)DATA.ini_balance = bal = DATA.balance;
                    break;
                }
            }
            if(bal > 0)color = "green";
            else if(bal == 0)color = "yellow";
            else color = "red";
            if(config.VERBOSE){
                DATA.reportData.push(chalk.white("Just fetched Your Info on Binance"));
                DATA.reportData.push(chalk.white("You "+chalk.green("ARE")+" allowed to trade!"));
                DATA.reportData.push(chalk.white(""+config.QUOTE+" balance is: ")+chalk[color](bal));
                DATA.reportData.push(chalk.white("Your Trading Fees are: ")+chalk.blue(config.FEETIERS[feeTier]));
                DATA.reportData.push(chalk.yellow("\nTHIS INFO IS ONLY VALID FOR BINANCE FUTURES: ")+ chalk.blue(config.USETESTNET?"testnet":"mainnet")+'\n');
            }

            DATA.feeTier = feeTier;
        };
        BINANCE.getSymbols = async function(params){
            const cli = params.cli;
            const tp = params.tp;
            var ts = params.ts;
            var symbols = [];
            var notionals = [];
            const res = await cli.getExchangeInfo().catch(err => { console.log(err); });
            for(var s=0;s<res.symbols.length;s++){
                if(res.symbols[s].contractType == tp && res.symbols[s].status == "TRADING" && config.MARKETS.includes(res.symbols[s].symbol)){
                    if(config.DEBUG)DATA.reportData.push(chalk.white("FOUND SYMBOL: ")+chalk.blue(res.symbols[s].symbol));
                    symbols.push(res.symbols[s].symbol);
                    notionals.push(+res.symbols[s].filters[5].notional);
                }
            }
            if(config.DEBUG)DATA.reportData.push("Found in Total: ["+chalk.blue(symbols.length)+"] Symbols!");
            return {
                symbols : symbols,
                notionals : notionals
            };
        };
        BINANCE.updateTrades = function(msg){
            const m = DATA.marketsList[DATA.marketsList.indexOf(msg.s)];

            for(var t=0;t<msg.length;t++)if(msg[t])DATA.markets[markets[m]].trades[t] = msg[t];
            for(var T=0;T<msg.length;T++)DATA.markets[markets[m]].trades[t]
            //console.log(DATA.markets[market].lastTrade);
        };
        BINANCE.setLeverage = async function(params){
            const cli = params.cli;
            const symbol = params.sy;
            const nots = await cli.getNotionalAndLeverageBrackets({symbol: symbol}).catch(err => { DATA.reportData.push(err); });

            let lv;
            if(config.USEMAXLEVERAGE && config.LEVERAGE >= nots[0].brackets[0].initialLeverage)lv = nots[0].brackets[0].initialLeverage;
            else lv = config.LEVERAGE;

            DATA.markets[symbol].lev = lv;

            if(config.DEBUG)DATA.reportData.push("SETTING LEVERAGE TO: " + chalk.yellow(lv) + " ON " + chalk.magenta(symbol));

            var rec = {};
            rec.method = "setLeverage";
            rec.object = {
                symbol : symbol,
                leverage : lv
            };

            if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
        };
        BINANCE.setMargin = function(params){
            const cli = params.cli;
            const mrg = params.mrg;
            const symbol = params.sy;

            if(config.DEBUG)DATA.reportData.push("SETTING MARGIN TYPE TO: " + chalk.yellow(mrg) + " ON " + chalk.magenta(symbol));

            var rec = {};
            rec.method = "setMarginType";
            rec.object = {
                symbol : symbol,
                marginType : mrg
            };

            if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
        };
        BINANCE.setMarkets = async function(params){
            const cli = params.cli;
            const tp = params.tp;
            const nt = params.nt;
           const res = await cli.getExchangeInfo().catch(err => { console.log(err); });
            for(var m=0;m<DATA.marketsList.length;m++){
                for(var s=0;s<res.symbols.length;s++){
                    if(DATA.marketsList[m] == res.symbols[s].symbol /*&& DATA.mkts.includes(res.symbols[s].symbol)*/){
                        if(config.DEBUG)DATA.reportData.push("CREATING MARKET FOR SYMBOL: "+chalk.magenta(DATA.marketsList[m]));
                        BINANCE.createMarket(DATA.marketsList[m]);
                        
                        DATA.markets[DATA.marketsList[m]].minNotional = +nt[m];
                        DATA.markets[DATA.marketsList[m]].tickSize = +res.symbols[s].filters[0].tickSize;
                        DATA.markets[DATA.marketsList[m]].pricePrecision = +res.symbols[s].pricePrecision;
                        DATA.markets[DATA.marketsList[m]].baseAssetPrecision = +res.symbols[s].baseAssetPrecision;
                        DATA.markets[DATA.marketsList[m]].quantityPrecision = +res.symbols[s].quantityPrecision;

                        if(config.CHANGEMARGIN)await BINANCE.setMargin({
                            cli : usdmClient,
                            mrg : config.MARGIN,
                            sy : DATA.marketsList[m]
                        });

                        if(config.CHANGELEVERAGE)await BINANCE.setLeverage({
                            cli : usdmClient,
                            lv : config.LEVERAGE,
                            sy : DATA.marketsList[m]
                        });
                        break;
                    }
                }
            }


        };
        BINANCE.updateSubsBook = function(msg){

            const market = msg.s; 

            for(var a=0;a<msg.a.length;a++)if(msg.a[a])DATA.markets[market].asks[a] = msg.a[a];
            for(var b=0;b<msg.b.length;b++)if(msg.b[b])DATA.markets[market].bids[b] = msg.b[b];
            if(DATA.markets[market].asks[0][0] && DATA.markets[market].bids[0][0])DATA.markets[market].price = +(((+DATA.markets[market].bids[0][0])+(+DATA.markets[market].asks[0][0]))/2).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize));

            DATA.markets[market].prices.push(DATA.markets[market].price);
            if(DATA.markets[market].prices.length > config.PRICETICKS)DATA.markets[market].prices.shift();

            if(DATA.markets[market].started)BINANCE.decide(market);
     
        };
        BINANCE.cancelAllOpenOrdersOn = async function(params){
            const cli = params.cli;
            const symbol = params.sy;

            if(config.DEBUG)DATA.reportData.push(chalk.yellow("CANCELLING") + " ALL ORDERS ON " + chalk.magenta(symbol));


            const res = await cli.cancelAllOpenOrders({ symbol : symbol }).catch(err => { DATA.reportData.push(err); });

        };
        BINANCE.cancelAllOpenOrders = async function(params){
            const cli = params.cli;

            for(var m=0;m<DATA.marketsList.length;m++){
                if(config.DEBUG)DATA.reportData.push(chalk.yellow("CANCELLING") + " ALL ORDERS ON " + chalk.magenta(DATA.marketsList[m]));
                var rec = {};
                rec.method = "cancelAllOpenOrders";
                rec.object = {
                    symbol : DATA.marketsList[m]
                };

                if(config.PLACEORDERS)BINANCE.buffer[BINANCE.buffer.length] = rec;
            }
        };
        BINANCE.closeAllPositions = async function(params){
            const cli = params.cli;

            if(config.CLOSEALLPOSITIONS)if(config.VERBOSE)DATA.reportData.push("\nWill now " + chalk.yellow(" CLOSE ") + " all Positions on EVERY market with\n" + chalk.magenta(config.QUOTE) + " as a Quote!\n\n");

            let res1;
            if(config.CLOSEALLPOSITIONS)res1 = await cli.getPositions().catch(error => { console.log(error.message); });

            if(config.CLOSEALLPOSITIONS){

                if(res1 != undefined)for(var m=0;m<res1.length;m++){
                    if(+res1[m].positionAmt < 0){
                        if(config.VERBOSE)DATA.reportData.push(chalk.yellow(" CLOSING ") + chalk.red(" SHORT ")  + "position of " + chalk.magenta(res1[m].positionAmt) + " on [" + chalk.magenta(res1[m].symbol) + "]...");                

                        DATA.LASTCLOSE = {
                            symbol : res1[m].symbol,
                            side : "BUY",
                            type : "LIMIT",
                            quantity : ''+(-1*(+res1[m].positionAmt)),
                            price : (+res1[m].entryPrice).toFixed(2),
                            timeInForce : "GTC",
                            reduceOnly : true
                        };

                        await cli.submitNewOrder({
                            symbol : res1[m].symbol,
                            side : "BUY",
                            type : "MARKET",
                            quantity : ''+(-1*(+res1[m].positionAmt)),
                            reduceOnly : true
                        }).catch(error => { 

                            console.log(error.message);

                            usdmClient.submitNewOrder(DATA.LASTCLOSE).catch(error => { console.log(error.message); });

                        });

                        //DATA.markets[res1[m].symbol].inShort = false;
                    
                    } else if(+res1[m].positionAmt > 0){
                        if(config.VERBOSE)DATA.reportData.push(chalk.yellow(" CLOSING ") + chalk.green(" LONG ")  + "position of " + chalk.magenta(res1[m].positionAmt) + " on [" + chalk.magenta(res1[m].symbol) + "]...");                

                        DATA.LASTCLOSE = {
                            symbol : res1[m].symbol,
                            side : "SELL",
                            type : "LIMIT",
                            quantity : ''+(-1*(+res1[m].positionAmt)),
                            price : (+res1[m].entryPrice).toFixed(2),
                            timeInForce : "GTC",
                            reduceOnly : true
                        };

                        await cli.submitNewOrder({
                            symbol : res1[m].symbol,
                            side : "SELL",
                            type : "MARKET",
                            quantity : res1[m].positionAmt,
                            reduceOnly : true
                        }).catch(error => { console.log(error.message);

                            usdmClient.submitNewOrder(DATA.LASTCLOSE).catch(error => { console.log(error.message); });

                        });

                        //DATA.markets[res1[m].symbol].inLong = false;

                    }
                 
                } else {

                    setTimeout(function(){
                        BINANCE.closeAllPositions({ cli : usdmClient });
                    }, 5000);

                }  

            } 

            if(config.VERBOSE)DATA.reportData.push(chalk.yellow("\n\nDONE!\n"));
        };
        BINANCE.handleOrderUpdate = async function(params){
            const market = params.s;
            const type = params.o;
            const side = params.S;
            const quantity = +params.q;
            const price = +params.p;
            const execution = params.x;
            const executed = params.X;

            /*if(type == "LIMIT" && executed == "PARTIALLY_FILLED" && DATA.markets[market] != undefined){
                if(config.VERBOSE)DATA.reportData.push(chalk.blue("LIMIT PARTIALLY_FILLED")+", Reacting...");
                if(side == "BUY"){
                    if(config.VERBOSE)DATA.reportData.push("SETTING "+chalk.blue("BUY")+" PARTIAL TWIN...");
                    DATA.markets[market].sell((price+price*2*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)), quantity.toFixed(DATA.markets[market].quantityPrecision), false, false, "LIMIT", market);
                } else if(side == "SELL"){
                    if(config.VERBOSE)DATA.reportData.push("SETTING "+chalk.blue("SELL")+" PARTIAL TWIN...");
                    DATA.markets[market].buy((price-price*2*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)), quantity.toFixed(DATA.markets[market].quantityPrecision), false, false, "LIMIT", market);
                }
            } else if(type == "LIMIT" && executed == "FILLED" && DATA.markets[market] != undefined){
                if(config.VERBOSE)DATA.reportData.push(chalk.blue("LIMIT FILLED")+", Reacting...");
                if(side == "BUY"){
                    if(config.VERBOSE)DATA.reportData.push("SETTING "+chalk.blue("BUY")+" TWIN...");
                    DATA.markets[market].sell((price+price*2*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)), quantity.toFixed(DATA.markets[market].quantityPrecision), false, false, "LIMIT", market);
                } else if(side == "SELL"){
                    if(config.VERBOSE)DATA.reportData.push("SETTING "+chalk.blue("SELL")+" TWIN...");
                    DATA.markets[market].buy((price-price*2*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)), quantity.toFixed(DATA.markets[market].quantityPrecision), false, false, "LIMIT", market);
                }
            }*/

            if(type == "LIMIT" && executed == "FILLED" && DATA.markets[market] != undefined){
                await BINANCE.cancelAllOpenOrdersOn({
                    cli : usdmClient,
                    sy : market
                });

                if(side == "BUY"){
                    //DATA.markets[market].order = 1;
                    //DATA.markets[market].order++;
                    //DATA.markets[market].order>=0?DATA.markets[market].order++:DATA.markets[market].order=0;

                    if(DATA.markets[market].order >= config.MAXORDERS){
                        //DATA.markets[market].stopMarket(market, "SELL", (price-price*config.MAXLOSS).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)));
                        //DATA.markets[market].takeMarket(market, "SELL", (price+price*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)));            
                    } 
                                    
                    if(config.VERBOSE)DATA.reportData.push(chalk.blue("|")+(''+DATA.markets[market].order).padStart(3 , DATA.markets[market].order>0?"0":"-")+chalk.blue("|") + chalk.green(" BUY ") + "FILLED!! on ["+chalk.magenta(market)+"]... Reacting...");

                    if(DATA.markets[market].order > 0){
                        DATA.markets[market].inShort = false;
                        DATA.markets[market].inLong = true;
                        //DATA.markets[market].acumulated = DATA.markets[market].order?DATA.markets[market].order>0?(DATA.markets[market].order+1)*DATA.markets[market].position:(-DATA.markets[market].order+1)*DATA.markets[market].position:DATA.markets[market].position;
                    } else if(DATA.markets[market].order == 0){
                        DATA.markets[market].inShort = false;
                        DATA.markets[market].inLong = false;
                        //DATA.markets[market].setGrid(market);
                    } else if(DATA.markets[market].order < 0){
                        DATA.markets[market].inShort = true;
                        DATA.markets[market].inLong = false;
                    } 
                } else if(side == "SELL"){
                    //DATA.markets[market].order = -1;
                    //DATA.markets[market].order--;
                    //DATA.markets[market].order<=0?DATA.markets[market].order--:DATA.markets[market].order=0;

                    if(DATA.markets[market].order <= -config.MAXORDERS){
                        //DATA.markets[market].stopMarket(market, "BUY", (price+price*config.MAXLOSS).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)));
                        //DATA.markets[market].takeMarket(market, "BUY", (price-price*config.MINPROFIT).toFixed(TOOLS.getPrecision(DATA.markets[market].tickSize)));
                    }
                                        
                    if(config.VERBOSE)DATA.reportData.push(chalk.blue("|")+(''+DATA.markets[market].order).padStart(3 , DATA.markets[market].order>0?"0":"-")+chalk.blue("|") + chalk.red(" SELL ") + "FILLED!! on ["+chalk.magenta(market)+"]... Reacting...");
                
                    if(DATA.markets[market].order > 0){
                        DATA.markets[market].inShort = false;
                        DATA.markets[market].inLong = true;
                        //DATA.markets[market].acumulated = DATA.markets[market].order?DATA.markets[market].order>0?(DATA.markets[market].order+1)*DATA.markets[market].position:(-DATA.markets[market].order+1)*DATA.markets[market].position:DATA.markets[market].position;
                    } else if(DATA.markets[market].order == 0){
                        DATA.markets[market].inShort = false;
                        DATA.markets[market].inLong = false;
                        //DATA.markets[market].setGrid(market);
                    } else if(DATA.markets[market].order < 0){
                        DATA.markets[market].inShort = true;
                        DATA.markets[market].inLong = false;
                    }

        		}

                DATA.markets[market].setGrid(market);
            } else if(type == "STOP_MARKET" && executed == "FILLED" && DATA.markets[market] != undefined){
        		if(side == "BUY"){
        			if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.red(" STOP MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]");
        		} else if(side == "SELL"){
                    if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.red(" STOP MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]");
        		}
	        } else if(type == "TAKE_PROFIT_MARKET" && executed == "FILLED" && DATA.markets[market] != undefined){
                if(side == "BUY"){
                    if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.green(" TAKE PROFIT MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]");
                } else if(side == "SELL"){ 
                    if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.green(" TAKE PROFIT MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]");
                }
            } else if(type == "MARKET" && executed == "FILLED" && DATA.markets[market] != undefined){
                if(side == "BUY"){
                    if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.blue(" MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]...");
                } else if(side == "SELL"){
                    if(config.VERBOSE)DATA.reportData.push(chalk.cyan(side) + chalk.blue(" MARKET ") + "TRIGGERED!! on ["+chalk.magenta(market)+"]...");
                }
                if(DATA.markets[market].order > 0){
                    DATA.markets[market].inShort = false;
                    DATA.markets[market].inLong = true;
                } else if(DATA.markets[market].order == 0){
                    DATA.markets[market].inShort = false;
                    DATA.markets[market].inLong = false;
                    DATA.markets[market].setGrid(market);
                } else if(DATA.markets[market].order < 0){
                    DATA.markets[market].inShort = true;
                    DATA.markets[market].inLong = false;
                }
            }
            //console.log(params);
        };
        BINANCE.CREATE = {};
        BINANCE.decide = async function(mk){
            /*const ask = +DATA.markets[mk].asks[0][0];
            const bid = +DATA.markets[mk].bids[0][0];
            const pos = DATA.markets[mk].position;
            const pos_2 = ((+pos)*2).toFixed(TOOLS.getPrecision(DATA.markets[mk].quantityPrecision));
            const inLong = DATA.markets[mk].inLong;
            const inShort = DATA.markets[mk].inShort;

            if(inShort){
                if(ask+ask*config.TAKEPROFIT < DATA.markets[mk].shortTrigger){
                    if(config.DEBUG)DATA.reportData.push(chalk.magenta(mk)+chalk.red(" SHORT ")+chalk.blue("TRIGGER UPDATED ") + "from ["+chalk.cyan(DATA.markets[mk].shortTrigger)+"] to ["+chalk.cyan(DATA.markets[mk].shortTrigger-DATA.markets[mk].shortTrigger*config.STOPLOSS)+"]");
                    DATA.markets[mk].shortTrigger -= DATA.markets[mk].shortTrigger*config.TAKEPROFIT;
                    DATA.markets[mk].shortTrigger = DATA.markets[mk].shortTrigger.toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));
                } else if(ask >= DATA.markets[mk].shortTrigger){
                    if(config.DEBUG)DATA.reportData.push(chalk.magenta(mk)+chalk.red(" SHORT ")+chalk.blue("INVERTED ") + "@ ["+chalk.cyan(ask)+"] with ["+chalk.cyan(pos)+"]");
                    DATA.markets[mk].buy(ask, pos, false, false, "MARKET", mk); 
                    DATA.markets[mk].inShort = false;
                    DATA.markets[mk].inLong = false;
                }
            }
            if(inLong){
                if(bid-bid*config.TAKEPROFIT > DATA.markets[mk].longTrigger){
                    if(config.DEBUG)DATA.reportData.push(chalk.magenta(mk)+chalk.green(" LONG ")+chalk.blue("TRIGGER UPDATED ") + "from ["+chalk.cyan(DATA.markets[mk].longTrigger)+"] to ["+chalk.cyan(DATA.markets[mk].longTrigger+DATA.markets[mk].longTrigger*config.STOPLOSS)+"]");
                    DATA.markets[mk].longTrigger += DATA.markets[mk].longTrigger*config.TAKEPROFIT;
                    DATA.markets[mk].longTrigger = DATA.markets[mk].longTrigger.toFixed(TOOLS.getPrecision(DATA.markets[mk].tickSize));
                } else if(bid <= DATA.markets[mk].longTrigger){
                    if(config.DEBUG)DATA.reportData.push(chalk.magenta(mk)+chalk.green(" LONG ")+chalk.blue("INVERTED ") + "@ ["+chalk.cyan(bid)+"] with ["+chalk.cyan(pos)+"]");
                    DATA.markets[mk].sell(bid, pos, false, false, "MARKET", mk); 
                    DATA.markets[mk].inLong = false;
                    DATA.markets[mk].inShort = false;
                }
            }*/

        };
        BINANCE.checkProfit = async function(){

            // TO FIX!!!

//console.log(DATA.balance, DATA.ini_balance, DATA.fpnl, config.TAKEPROFIT);

            var cycles = JSON.parse(fs.readFileSync("./cycles.json", 'utf8'));
            DATA.cycle[0] = cycles.profit;
            DATA.cycle[1] = cycles.loss;

            if(!isNaN(DATA.fpnl) && DATA.balance-DATA.ini_balance+DATA.fpnl >= config.TAKEPROFIT*DATA.ini_balance){
                /*
		DATA.ini_balance = DATA.balance;
                DATA.profit = 0;
                DATA.fpnl = 0;
                DATA.cycle[0]++;
                if(config.VERBOSE)DATA.reportData.push("Bot just Hit TAKEPROFIT of ["+chalk.green(config.TAKEPROFIT)+" %] for this session!\n Will Refresh!...\n");
                await BINANCE.cancelAllOpenOrders({
                    cli : usdmClient
                });

                await BINANCE.closeAllPositions({
                    cli : usdmClient
                });

                for(var m=0;m<DATA.marketsList.length;m++){
                    DATA.markets[DATA.marketsList[m]].order = 0;
                    DATA.markets[DATA.marketsList[m]].inLong = false;
                    DATA.markets[DATA.marketsList[m]].inShort = false;

                    
                    DATA.markets[DATA.marketsList[m]].setGrid(DATA.marketsList[m]);
                }
                */
                cycles.profit++;
                fs.writeFileSync("./cycles.json", JSON.stringify(cycles),'utf8')
		throw Error(chalk.green("REFRESHING"));
	    }

            if(!isNaN(DATA.fpnl) && DATA.balance-DATA.ini_balance+DATA.fpnl < -config.STOPLOSS*DATA.ini_balance){
                /*
		DATA.ini_balance = DATA.balance;
                DATA.profit = 0;
                DATA.fpnl = 0;
                DATA.cycle[1]++;
                if(config.VERBOSE)DATA.reportData.push("Bot just Hit STOPLOSS of ["+chalk.red(config.STOPLOSS)+" %] for this session!\n Will Refresh!...\n");
                await BINANCE.cancelAllOpenOrders({
                    cli : usdmClient
                });

                await BINANCE.closeAllPositions({
                    cli : usdmClient
                });

                for(var m=0;m<DATA.marketsList.length;m++){
                    DATA.markets[DATA.marketsList[m]].order = 0;
                    DATA.markets[DATA.marketsList[m]].inLong = false;
                    DATA.markets[DATA.marketsList[m]].inShort = false;

                    DATA.markets[DATA.marketsList[m]].setGrid(DATA.marketsList[m]);
                }
		*/
                cycles.loss++;
                fs.writeFileSync("./cycles.json", JSON.stringify(cycles),'utf8')
		throw Error(chalk.red("REFRESHING"));
            }
        };

function subsctibe(){
    if(config.DEBUG)DATA.reportData.push("Will now Subscribe to Channels...");
    wsClient.subscribeUsdFuturesUserDataStream(config.USETESTNET);
    for(var m=0;m<DATA.marketsList.length;m++){
        wsClient.subscribeMarkPrice(DATA.marketsList[m], config.USETESTNET?'usdmTestnet':'usdm', 1000, true);
        wsClient.subscribePartialBookDepths(DATA.marketsList[m], config.MARKETSDEPTH, config.MARKETSUPDATE, config.USETESTNET?'usdmTestnet':'usdm');
    }
}

async function main(){
    console.clear();
    console.log("LOADING...\nhold on...\n");
    setInterval(TOOLS.setTime,1000);
    BINANCE.sendBuffer();

    await BINANCE.getAccountInfo({
        cli : usdmClient
    });

    var temp = await BINANCE.getSymbols({
        cli : usdmClient, 
        tp : "PERPETUAL", 
        ts : config.MAXMARKETS
    });

    DATA.marketsList = temp.symbols;
    fs.writeFileSync("mkts.txt",DATA.marketsList.join('\n'),'utf8');

    if(config.CANCELLALLORDERS)await BINANCE.cancelAllOpenOrders({
        cli : usdmClient
    });

    if(config.CLOSEALLPOSITIONS)await BINANCE.closeAllPositions({
        cli : usdmClient
    });

    DATA.POSITIONPERCENTAGE = +(1/(config.MAXORDERS*config.MARKETS.length)*config.MAXRISK).toFixed(config.DECIMALS);
    //if(!config.USEMAXLEVERAGE && DATA.POSITIONPERCENTAGE*DATA.balance*config.LEVERAGE < config.MINPOSITION)DATA.POSITIONPERCENTAGE = +(config.MINPOSITION/DATA.balance/config.LEVERAGE).toFixed(config.DECIMALS);

    DATA.threads = DATA.marketsList.length;

    await BINANCE.setMarkets({
        cli : usdmClient, 
        tp : "PERPETUAL",
        nt: temp.notionals
    });

    if(config.DEBUG)setInterval(TOOLS.log,config.LOGTIME);
    else setInterval(TOOLS.getReport,config.REFRESHTIME);

    setTimeout(function(){
        //setInterval(BINANCE.checkProfit,config.REFRESHTIME);
    },30000);

    subsctibe();
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Run -----------------------------------------------------------------------------------------------------------------------------

main();   

// ---------------------------------------------------------------------------------------------------------------------------------
// Tests ---------------------------------------------------------------------------------------------------------------------------

async function test(){
    const res = await usdmClient.getBalance();
    //usdmClient.getBalance();

    console.log(res);
}

//test();

// ---------------------------------------------------------------------------------------------------------------------------------

