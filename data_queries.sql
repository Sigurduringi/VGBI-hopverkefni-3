
/* 
Hægt skal vera að skoða fjölda seldra eininga niður á 
ár, mánuð, viku eða dag eftir búð og vöru:
*/

SELECT SUM(unitsSold) FROM [H9].factSales
INNER JOIN [H9].dimCalendar ON [H9].factSales.date = [H9].dimCalendar.date
WHERE year = 2021 and monthName = 'april' and idStore = 3;



-- Eftir búð:
SELECT SUM(unitsSold) FROM [H9].factSales
WHERE idStore = 5;




/*
Hægt skal vera að skoða veltu og kostnað með sama niðurbroti
*/

SELECT SUM(cost)
FROM [H9].factSales
INNER JOIN [H9].dimCalendar ON [H9].factSales.date = [H9].dimCalendar.date
INNER JOIN [H9].dimProduct  ON [H9].factSales.idProduct = [H9].dimProduct.id
-- T.d. kostnaður í maí 2021 í búð 2:
where year= 2021 and monthName = 'may' and idStore= 2;



/*
Hægt skal vera að reikna meðal veltu, meðal upphæð körfu og 
meðal fjölda keyptra hluta per körfu.*/

--meðalvelta:
SELECT AVG(turnover) AS averageTurnover
FROM (
  SELECT receipt, SUM(unitsSold * price) AS turnover
  FROM [H9].factSales
  INNER JOIN [H9].dimProduct ON [H9].factSales.idProduct = [H9].dimProduct.id
  GROUP BY receipt
) AS subquery;



/*
Það þarf að vera hægt að skoða lager upplýsingar niður á búð og vöru.

DÆMI - Hve margar einingar af vörunni “Action Figure” 
eru til í versluninni “Maven Toys Puebla 2”:
*/

SELECT InStock FROM [H9].factInventory
INNER JOIN [H9].dimStores ON [H9].factInventory.idStore = [H9].dimStores.id
INNER JOIN [H9].dimProduct ON [H9].factInventory.idProduct = [H9].dimProduct.id
WHERE dimStores.name = 'Maven Toys Puebla 2' AND dimProduct.name = 'Action Figure';


-- Gæðavandamálatöflur;


