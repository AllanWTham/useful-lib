/* Create a cas session called 'mysess' with 30 mins timeout */
cas mysess sessopts=(caslib=casuser timeout=1800 locale="en_US" metrics=True);

/* Shows notes, warnings, and errors,plus informational messages
that can help you understand more about SAS's internal processing
(e.g., index usage, optimizer decisions, etc.)
*/
options msglevel=i;


/* Load and replace 'sashelp.cars' to 'casuser.mycars' etc. */
proc casutil;
  droptable incaslib=casuser casdata="MYcaRs" quiet;
  load data=sashelp.cars outcaslib="casuser" casout="mycars" replace;
quit;

/* Assign the libref to 'CASUSER'.
   Note, that this allow traditional data steps to access CAS tables.
*/
libname mycas cas caslib='CASUSER';


/* ---------- Running data steps in CAS (or not) ---------- */

/* This one is not run in CAS */
data work.cars1;
  set mycas.mycars;
  where make='Audi';
run;

/* This one is run in CAS */
data mycas.cars2;
  set mycas.mycars;
  where make='Audi';
run;


/* ---------- Method 1 - Using traditional data step ---------- */
data mycas.cars2;
   set mycas.mycars;
   where make = 'Audi';
   MSRP_Tax1 = MSRP * 1.08;
run;

/* ---------- Method 2 - Using Proc CAS ---------- */
proc cas;
  mytable={caslib="casuser", name="mycars_discount"};
  source myds;
    data casuser.mycars_discount;
      set casuser.mycars;
      discount=msrp*.90;
      where origin='Europe';
      keep make model msrp discount;
    run;
  endsource;

  /* Use the runcode */
  datastep.runcode / code=myds;

  /* View the table and column info and the content */
  table.tableinfo / caslib=mytable.caslib;
  table.columninfo / table=mytable;
  table.fetch / table=mytable, to=10;
quit;


/* ---------- Cleaning existing CAS tables ---------- */

/* Delete any leftover tables and start over.
   If you have more tables in your personal 'casuser' caslib to remove,
   feel free to plug into the list in mytables
 */
proc cas;
  mytables={"cars2", "mycars_discount"};
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