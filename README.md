# Taiwan Stock Exchange Crawler

原作來源
https://github.com/Asoul/tsec

這是一個去爬 台灣證券交易所 和 證券櫃檯買賣中心 的爬蟲，秉持著 Open Data 的理念，公開爬蟲公開資料最安心。

### Note
*  不包含股票歷史資料, 請自行尋找
*  爬蟲一次僅能爬約20-30天資料, 無法一次爬完. 不建議使用此套件下載所有歷史行情

### 修改內容 
* 證交所網頁格式更動, 修改了爬蟲的參數
* 新增塞入資料庫功能, 使用sqlalchemy, 先進df再進db的方式
* 採用單一檔案儲存csv
