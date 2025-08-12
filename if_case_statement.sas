/* This code filters using if and case statements.
*/

/* Create a cas session called 'mysess' with 30 mins timeout */
cas mysess sessopts=(caslib=casuser timeout=1800 locale="en_US" metrics=True);

/* Shows notes, warnings, and errors,plus informational messages
that can help you understand more about SAS's internal processing
(e.g., index usage, optimizer decisions, etc.)
*/
options msglevel=i;

/* Assign the libref to 'CASUSER'.
   Note, that this allow traditional data steps to access CAS tables.
*/
libname mycas cas caslib='CASUSER';

/* ---------- Loading Client Side file using PROC HTTP ---------- */

/* Load a CSV file into memory as CAS table. Note, this is client side loading */
%let data='http://support.sas.com/documentation/onlinedoc/viya/exampledatasets/iris.csv';
filename t temp;   
proc http method="get" url=&data. out=t;                
run;
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));  

proc cas;
   upload path=&temppath.
        casOut={
        name="iris"                                      
        replace=True
      }
     importOptions={fileType="csv"}  
;
quit;

/* Load and replace 'sashelp.cars' to 'casuser.mycars' etc. */
proc casutil;
  droptable incaslib=casuser casdata="MYcaRs" quiet;
  load data=sashelp.cars outcaslib="casuser" casout="mycars" replace;
run;

/* Obtaining Table level info for "CASUSER" */
proc cas;
  table.tableInfo / caslib="casuser";
quit;

/* ---------- Change values using if statement ---------- */
proc cas;
   dataStep.runCode / code='
      data casuser.iris_abbrev;
         set casuser.iris;
         length SpeciesAbbr $3;
         if upcase(Species) = "SETOSA" then SpeciesAbbr = "SET";
         else if upcase(Species) = "VERSICOLOR" then SpeciesAbbr = "VER";
         else if upcase(Species) = "VIRGINICA" then SpeciesAbbr = "VIR";
         else SpeciesAbbr = "UNK";
      run;
   ';
   table.fetch / table={caslib="casuser", name="iris_abbrev"}, to=5;
quit;

/* ---------- Change values using case statement ---------- */
data mycas.iris_abbrev2;
  set mycas.iris;
    length SpeciesAbbr $5;
    select (upcase(Species));
      when ("SETOSA") SpeciesAbbr = "SETO";
      when ("VERSICOLOR") SpeciesAbbr = "VERS";
      when ("VIRGINICA") SpeciesAbbr = "VIRG";
      otherwise SpeciesAbbr = "UNK";
    end;
run;

/* Fetch a few observations */
proc cas;
   table.fetch / table={caslib="casuser", name="iris_abbrev2"}, to=5;
quit;

/* ---------- Change values using case statement using FedSQL ---------- */
proc fedsql sessref=mysess;
  create table casuser.cars_categorized as
  select make, model, msrp,
    case 
      when msrp > 50000 then 'High'
      when msrp > 30000 then 'Medium'
      else 'Low'
     end as category
  from casuser.mycars;
quit;

/* Fetch a few observations */
proc cas;
   table.fetch / table={caslib="casuser", name="cars_categorized"}, to=5;
quit;

/* ---------- Cleaning existing CAS tables ---------- */

/* Delete any leftover tables and start over.
   If you have more tables in your personal 'casuser' caslib to remove,
   feel free to plug into the list in mytables
 */
proc cas;
  mytables={"iris","iris_abbrev","iris_abbrev2",
            "mycars","cars_categorized"};
  do i over mytables;
    table.tableExists result=tbl / caslib="casuser", name=upcase(i);
    if tbl.exists>0 then do;
      table.dropTable / caslib="casuser", name=upcase(i);
    end;
  end;
  table.tableInfo / caslib="casuser";
quit;

/* Obtaining Table/file level info */
proc cas;
 mylib="casuser";
 table.tableInfo / caslib=mylib; /* CAS Table info */
 table.fileInfo / caslib=mylib; /* filesystem level info */
quit;


/* Terminate the current CAS session called 'mysess' */
cas mysess terminate;