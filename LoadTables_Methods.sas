/* There are three methods to loading tables:
   - Using PROC CASUTIL
   - Using PROC CAS with table.loadTable
   - Using traditional DATA step
   - Using PROC HTTP

The following code perform these tasks by loading to "CASUSER" caslib:
  - Method 1: Use PROC CASUTIL to load 'cars' dataset
  - Method 2: Use PROC CAS with table.loadTable to load 'class' dataset
  - Method 3: Use traditional data step to load 'heart' dataset
  - Method 4: Use PROC HTTP to load a CSV file (‘iris.csv’) hosted online
*/

/* Create a cas session called 'mysess' with 30 mins timeout */
cas mysess sessopts=(caslib=casuser timeout=1800 locale="en_US" metrics=True);

/* Assign the libref to 'CASUSER'.
   Note, that this allow traditional data steps to access CAS tables.
*/
libname mycas cas caslib='CASUSER';


/* ---------- Method 1 - Using Proc CASUTIL ---------- */

/* Load and promote 'sashelp.cars' to 'casuser.mycars' etc. */
proc casutil;
  droptable incaslib=casuser casdata="MYcaRs" quiet;
  load data=sashelp.cars outcaslib="casuser" casout="mycars" promote;
  droptable incaslib=casuser casdata="MYcLasS" quiet;
  load data=sashelp.class outcaslib="casuser" casout="MYcLasS" promote;
run;


/* ---------- Method 2 - Using Proc CAS ---------- */

/* Save different formats of the same dataset to filesystem
   and then use these files to load to CAS tables.
   Otherwise, there is no sample file in the filesystem to load.
*/
proc cas;
  intable={name="myclass", caslib="casuser"};
  table.save /
       table={name=intable.name, caslib=intable.caslib},
       caslib=intable.caslib, name="myclass_sas7bdat.sas7bdat",
       replace=True;
  table.save /
       table={name=intable.name, caslib=intable.caslib},
       caslib=intable.caslib, name="myclass_csv.csv",
       replace=True;
  table.save /
       table={name=intable.name, caslib=intable.caslib},
       caslib=intable.caslib, name="myclass_xlsx.xlsx",
       replace=True;
  table.save /
       table={name=intable.name, caslib=intable.caslib},
       caslib=intable.caslib, name="myclass_sashdat.sashdat",
       replace=True;
quit;

/* Load and promote various formats to "CASUSER" */

/* Server-side loading */
proc cas;
  mylib="casuser";
  table.loadTable /
       path="myclass_sas7bdat.sas7bdat", caslib=mylib,
       casout={caslib=mylib,
               name="myclass_sas7bdat",
               promote=TRUE
               };
  table.loadTable /
       path="myclass_csv.csv", caslib=mylib,
       casout={caslib=mylib,
               name="myclass_csv",
               promote=TRUE
               };
  table.loadTable /
       path="myclass_xlsx.xlsx", caslib=mylib,
       casout={caslib=mylib,
               name="myclass_xlsx",
               promote=TRUE
               };
  table.loadTable /
       path="myclass_sashdat.sashdat", caslib=mylib,
       casout={caslib=mylib,
               name="myclass_sashdat",
               promote=TRUE
               };
quit;


/* ---------- Method 3 - Using traditional data step ---------- */

/* Load and promote 'sashelp.heart' to 'casuser.myheart1' etc. */
data mycas.myheart1;
  set sashelp.heart;
run;

/* Load and promote 'sashelp.heart' to 'casuser.myheart2' etc. */
proc sql noprint;
  create table mycas.myheart2 as
    select * from sashelp.heart;
quit;


/* ---------- Method 4 - Using Proc HTTP ---------- */

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


/* ---------- Obtaining table/file level information ---------- */

/* Obtaining Table/file level info for "CASUSER" */
proc cas;
 mylib="casuser";
 table.tableInfo / caslib=mylib;
 table.fileInfo / caslib=mylib;
quit;

/* Fetch some contents */
proc cas;
  table.fetch / table={caslib="casuser", name="iris"}, to=10;
quit;

/* ---------- Cleaning up "CASUSER" filesystem path ---------- */

/* Remove the rest of the source files, 'myclass*' which are
   of different formats.
   Using do loop to remove them all. The table.fileInfo will show
   all source files are removed.
*/
proc cas;
  myfiles={"myclass_sas7bdat.sas7bdat",
           "myclass_csv.csv",
           "myclass_xlsx.xlsx",
           "myclass_sashdat.sashdat"};
  mylib="casuser";
  do file over myfiles;
    table.deletesource /
         source=file, caslib=mylib;
  end;
  /* View the 'casuser' data source */
  table.fileinfo / caslib=mylib;
quit;


/* ---------- Cleaning existing CAS tables ---------- */

/* Delete any leftover tables and start over.
   If you have more tables in your personal 'casuser' caslib to remove,
   feel free to plug into the list in mytables
 */
proc cas;
  mytables={"myheart1","myheart2","iris","mycars","myclass",
            "myclass_sas7bdat","myclass_csv",
            "myclass_xlsx","myclass_sashdat"};
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