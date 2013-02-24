-- PREREQ
-- ALTER TABLE patstat201204.TLS212_CITATION ADD INDEX _p(cited_pat_publn_id);


-- In allegato trovi il file con il PUBLN_NR delle patenti CITATE 
-- per cui vorrei le FOREWARD CITATIONS: le “patents” CHE-CITANO 
-- le PUBLN_NR in allegato.
 

-- carico i dati di input 
drop table if exists PUBLN_NR;
create temporary table PUBLN_NR (
       publn_auth CHAR(2),
       publn_nr   INT,
       publn_kind CHAR(2),
       KEY _a(publn_auth),
       KEY _n(publn_nr) )
ENGINE=MyISAM  DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

truncate table PUBLN_NR;
load data local infile '/tmp/PUBLN_NR.txt' 
into table  PUBLN_NR
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(@var)
set
publn_auth=left(@var,2),
publn_nr=right(@var,8),
publn_kind='A1'
;

-- STEP 1 (da PUBLN_NR a PAT_PUBLN_ID)
--        estrazione da TLS211 dei PAT_PUBLN_ID che 
--        corrispondono a PUBL_NR nel file allegato
--        queste sono le patenti CITATE di interesse
drop table if exists PUBLN_ID;
create table PUBLN_ID 
select publn_auth,publn_nr,A.publn_kind,appln_id,pat_publn_id 
from itaappdb.TLS211_PAT_PUBLN A join PUBLN_NR B using (publn_auth,publn_nr);
alter table PUBLN_ID add index _p(pat_publn_id);


-- STEP 2 (estrazione delle citande)
--        estrazione da TLS212 dei record dove vale 
--        la condizione PAT_PUBLN_ID=CITED_ PAT_PUBLN_ID
--        ogni record corrisponde quindi a una patente CHE-CITA
drop table if exists CITING;
create table CITING (
       cited_publn_id int,
       cited_prior_date date,
       citing_publn_id int,
       citing_prior_date date,
       key _a(citing_publn_id),
       key _p(cited_publn_id)
)
select distinct B.pat_publn_id as cited_publn_id, NULL as cited_prior_date, A.pat_publn_id as citing_publn_id, NULL as citing_prior_date
from patstat201204.TLS212_CITATION A join PUBLN_ID B on (cited_pat_publn_id=B.pat_publn_id);


-- STEP 3 (cerco le informazioni sulle date)
--         aggiungiamo due colonne all’estrazione: 
--         la PUBL_DATE delle patents CITATE e CHE-CITANO

-- Per fare questo tro o prima l'appln_id dei brevetti citati
drop table if exists CITED_APPLN;
create temporary table CITED_APPLN
select distinct A.pat_publn_id as cited_publn_id, A.appln_id as cited_appln_id 
from patstat201204.TLS211_PAT_PUBLN A join CITING B  on (B.cited_publn_id=pat_publn_id);
alter table CITED_APPLN add index _p(citing_appln_id);

-- calcolo la appln prior 
drop table if exists CITED_PRIOR_APPLN;
create temporary table CITED_PRIOR_APPLN
select distinct A.appln_id as appln_id, A.prior_appln_id as prior_appln_id, prior_appln_seq_nr from patstat201204.TLS204_APPLN_PRIOR A join CITED_APPLN B  on (B.cited_appln_id=appln_id)
group by appln_id having prior_appln_seq_nr=MAX(prior_appln_seq_nr);
alter table CITED_PRIOR_APPLN add index _p(appln_id);

-- prendo la appln prior o quella attuale se non ha prior (usa COALESCE)
drop table if exists CITED_PRIOR_DATE;
create temporary table CITED_PRIOR_DATE(
       appln_id int,
       prior_appln_id int,
       prior_date date,
       key _a(appln_id),
       key _p(prior_appln_id)
)
select B.cited_appln_id as appln_id , COALESCE(A.prior_appln_id,B.cited_appln_id) as prior_appln_id, NULL  As prior_date
from CITED_APPLN B left join PRIOR_APPLN A on (A.appln_id = B.cited_appln_id);

-- aggiorno la prior date
update CITED_PRIOR_DATE P, patstat201204.TLS201_APPLN A set P.prior_date=A.appln_filing_date where P.prior_appln_id=A.appln_id;

-- aggiorno la prior date nella tabella CITING
update CITING C, CITED_PRIOR_DATE P, CITED_APPLN A set C.cited_prior_date=P.prior_date where C.cited_publn_id=A.cited_publn_id and A.cited_appln_id=P.appln_id;

-- Ora per i brevetti citandi (i passi sono gli stessi del precedente)
drop table if exists CITING_APPLN;
create temporary table CITING_APPLN
select distinct A.pat_publn_id as citing_publn_id, A.appln_id as citing_appln_id 
from patstat201204.TLS211_PAT_PUBLN A join CITING B  on (B.citing_publn_id=pat_publn_id);
alter table CITING_APPLN add index _p(citing_appln_id);

-- calcolo la appln prior 
drop table if exists CITING_PRIOR_APPLN;
create temporary table CITING_PRIOR_APPLN
select distinct A.appln_id as appln_id, A.prior_appln_id as prior_appln_id, prior_appln_seq_nr from patstat201204.TLS204_APPLN_PRIOR A join CITING_APPLN B  on (B.citing_appln_id=appln_id)
group by appln_id having prior_appln_seq_nr=MAX(prior_appln_seq_nr);
alter table CITING_PRIOR_APPLN add index _p(appln_id);

-- prendo la appln prior o quella attuale se non ha prior (usa COALESCE)
drop table if exists CITING_PRIOR_DATE;
create temporary table CITING_PRIOR_DATE(
       appln_id int,
       prior_appln_id int,
       prior_date date,
       key _a(appln_id),
       key _p(prior_appln_id)
)
select B.citing_appln_id as appln_id , COALESCE(A.prior_appln_id,B.citing_appln_id) as prior_appln_id, NULL  as prior_date
from CITING_APPLN B left join CITING_PRIOR_APPLN A on (A.appln_id = B.citing_appln_id);

-- aggiorno la prior date
update CITING_PRIOR_DATE P, patstat201204.TLS201_APPLN A set P.prior_date=A.appln_filing_date where P.prior_appln_id=A.appln_id;

-- aggiorno la prior date nella tabella CITING
update CITING C, CITING_PRIOR_DATE P, CITING_APPLN A set C.citing_prior_date=P.prior_date where C.citing_publn_id=A.citing_publn_id and A.citing_appln_id=P.appln_id;

-- alla fine della procedura dovremmo avere una tabella con 4 campi
-- CITED_PAT_PUBLN_ID                       
-- DATE_CITED_ PAT_PUBLN_ID           
-- PAT_PUBLN_ID                                  
-- DATE_PAT_PUBLN_ID           

-- è la tabella CITING con campi
-- cited_publn_id, cited_prior_date
-- citinf_publn_id, citing_prior_date

-- Tabella per anno:

select 
year(citing_prior_date) as YearCiting,
year(cited_prior_date) as YearCited,
count(cited_publn_id) Ncited from CITING
       group by year(citing_prior_date),year(cited_prior_date);

-- VERIFICA

-- Trattandosi di FOREWARD CITATIONS dovrebbe essere verificata la condizione
-- DATA_ PAT_PUBLN_ID > DATA_ CITED_ PAT_PUBLN_ID  

-- 
select * from CITING where citing_prior_date<cited_prior_date;

-- la condizione non viene verificata


