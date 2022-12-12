# Parsovanie title, alt a iných špecifických údajov entity Movie
**Predmet:** Vyhľadávanie informácií  
**Autor:** Martin Rudolf  
**Cvičiaci:** Ing. Igor Stupavský  
  
  Cieľom projketu je navrhnúť a implementovať systémové riešenie na parsovanie filmov a ich špecifikácií (názov, meno režiséra, meno scenáristu, rok vydania, krajina pôvodu, žaner) z freebase.  
  
  ## Technológie
  **Nxparser** na spracovanie RDF N-Triples - použité vo verzii 1 (branch: feature/verzia-1)  
  **Json** na sracovanie JSON súborov  
  **Apache Lucene** na ingexovanie a vyhladavanie - použité vo verzii 1, 2  
  **Apache Spark** na distribuované spracovanie a parsovanie - použité vo verzií 2
  
  ## Dáta
  [Head 1 000 000](https://vi2022.ui.sav.sk/lib/exe/fetch.php?media=freebase-head-1000000.zip)  
  [Head 10 000 000](https://vi2022.ui.sav.sk/lib/exe/fetch.php?media=freebase-head-10000000.zip)  
  [Head 100 000 000](https://vi2022.ui.sav.sk/lib/exe/fetch.php?media=freebase-head-100000000.zip)  
  
  ## Verzia 2
  Pre parsovanie dát využívame framework **Apache Spark**, kvôli distribuovanému efektívnemu spracovaniu veľkého množstva dát.  Pre následnu Indexáciu využivame knižnicu **Apache Lucene**, ktorá nám služi aj na vyhľadávanie pomocou vytvorených indexov. Logika pre celý systém je definovaná v diagrame
  
  
