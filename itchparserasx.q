							/############################### User inputs ###############################

/files that have the date in middle-endian format, convert to big-endian.
/default date function works for YYYYMMDD format
/for middle-endian format files use the flag with the following argument for DDMMYYYY format
/ -datefunc "{neg[14h]\$ string[x][4 5 6 7 0 1 2 3]}"
/dfltdatefunc:{"D"$8# raze -1#"/" vs string x}
asxdatefunc:{"D"$"20",4_10#raze -1#"/" vs raze string x}

p:.Q.def[`init`exit`itchfile`cutsize`save`saveto`datefunc!(1b;1b;`$(string .z.d)[5 6 8 9 0 1 2 3],".PSX_ITCH_50";20000;1b;`HDB; enlist -3!asxdatefunc)].Q.opt .z.x
p[`datefunc]:value first p`datefunc;
p,:enlist[`date]!enlist p[`datefunc]p`itchfile;
if[0Nd=p`date;-2 "Error: null date - Please add date function to the command line";if[not `noexit in key p;exit[0]]];

usage:{-1 
  "
  ######################################### ITCH Parser #########################################################\n
  This script is used in order to convert ITCH messages into kdb+ tables. The sample usage is as follows:        \n
  q itchparserasx.q -init 1 -exit 1 -itchfile ../NTP_180304_1520148830.log -cutsize 20000 -save 1 -saveto HDBasx \n
  init is a boolean which tells q to parse the file provided automatically. The default value is 1               \n
  exit is a boolean which tells q to exit on completion of the parsing                                           \n
  date will be extracted from the filename using datefunc                                                        \n
  cutsize determines the number of syms which will be saved at any given time. It is important to match          \n
  to your systems specifications as too high a number will cause memory issues                                   \n
  save is a boolean which tells q to save the tables. It defaults to 1                                           \n
  saveto is the location where the tables are to be saved.                                                       \n
  This script can be used with slave threads. To start the script with slave threads use the flag -s             \n
  along with the number of cores you wish to use.                                                                \n"
  ;exit[0]}
if[`usage in key p; usage[]]
							/############################### Configuration ###############################

/This script is written using the specifications found in http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf.
/On updates to these specs, this is the section which requires editing. It is necessary that the order of the key is consistant for all of the dictionaries which use
/message type characters.

/The following contains a dictionary of casting functions which will be called via the types dictionary
typesf:(!) . flip 
  ((`alpha1;{first "c"$x});
   (`alpha;{"c"$x});
   (`price8;{256 sv x});
   (`delta;{256 sv x});
   (`numeric1;{256 sv x});
   (`numeric2;{256 sv x});
   (`numeric4;{256 sv x});
   (`numeric8;{256 sv x})
  )

msgoffsets:(!) . flip
  (("T";enlist 1);
   ("S";1 5 7);
   ("f";1 5 7 11 43 103 115 121 127 133 135 136 137 141 145 149 157 160 168 169 171 172 176);
   ("h";1 5 7 11 43 103 115 121 127 133 135 136 137 145 149 150 154 158 159 163 167 171 179 187 190 198 199 201 202 206 210);
   ("M";1 5 7 11 43 103 109 110 111 115 119 120 124 125 129 137 141 142 146 154 158 159 163 171 175 176 180 188 192 193 197 205 209 210 214);
   ("m";1 5 7 11 43 103 109 110 111 115 119 120 124 125 129 137 141 142 146 154 158 159 163 171 175 176 180 188 192 193 197 205 209 210 214 222 226 227 231 239 243 244 248 256 260 261 265 273 277 278 282 290 294 295 299 307 311 312 316 324 328 329 333 341 345 346 350 358 362 363 367 375 379 380 384 392 396 397 401 409 413 414 418 426 430 431 435 443 447 448 452);
   ("O";1 5 7 11);
   ("A";1 5 7 11 12 20 28 32);
   ("X";1 5 7 11 12 20);
   ("D";1 5 7 11 12);
   ("E";1 5 7 11 12 20 24 25 33 37 45 53);
   ("C";1 5 7 11 12 20 24 25 33 37 45);
   ("e";1 5 7 11 12 20 24 25 33 37 45 49 50 58);
   ("j";1 5 7 11 12 20 28 32);
   ("l";1 5 7 11 12 20 28 32);
   ("k";1 5 7 11 12);
   ("P";1 5 7 11 12 20 24 32 40 43);
   ("p";1 5 7 11 12 20 24 32 36 37 45 53 56 60 61 69 77);
   ("B";1 5 7 11);
   ("Z";1 5 7 11 19 27 35);
   ("t";1 5 7 11 19 27 35 43 47);
   ("Y";1 5 7 11 19 27 31);
   ("x";1 5 7 13);
   ("q";1 5 7 11 12);
   ("W";1 5 7 11 19 27 35 43 51);
   ("V";1 5 7 11 19 27);
   ("G";enlist 1)
  )

types:(!) . flip
  (("T";`numeric4);
   ("S";`numeric4`numeric2`alpha1);
   ("f";`numeric4`numeric2`numeric4`alpha`alpha`alpha`alpha`alpha`alpha`numeric2`numeric1`numeric1`numeric4`numeric4`numeric4`price8`alpha`numeric8`numeric1`numeric2`numeric1`numeric4`numeric4);
   ("h";`numeric4`numeric2`numeric4`alpha`alpha`alpha`alpha`alpha`alpha`numeric2`numeric1`alpha1`price8`numeric4`numeric1`numeric4`numeric4`numeric1`numeric4`numeric4`numeric4`price8`numeric8`alpha`numeric8`numeric1`numeric2`numeric1`numeric4`numeric4`alpha);
   ("M";`numeric4`numeric2`numeric4`alpha`alpha`alpha`numeric1`numeric1`numeric4`numeric4`numeric1`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8);
   ("m";`numeric4`numeric2`numeric4`alpha`alpha`alpha`numeric1`numeric1`numeric4`numeric4`numeric1`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8`numeric4`alpha1`numeric4`price8);
   ("O";`numeric4`numeric2`numeric4`alpha1);
   ("A";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric8`numeric4`price8);
   ("X";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4);
   ("D";`numeric4`numeric2`numeric4`alpha1`numeric8);
   ("E";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4`alpha1`numeric8`numeric4`price8`numeric8`alpha);
   ("C";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4`alpha1`numeric8`numeric4`price8`numeric8);
   ("e";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4`alpha1`numeric8`numeric4`price8`numeric4`alpha1`numeric8`numeric8);
   ("j";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric8`numeric4`price8);
   ("l";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric8`numeric4`price8);
   ("k";`numeric4`numeric2`numeric4`alpha1`numeric8);
   ("P";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4`price8`numeric8`alpha`alpha);
   ("p";`numeric4`numeric2`numeric4`alpha1`numeric8`numeric4`price8`numeric4`alpha1`numeric8`numeric8`alpha`numeric4`alpha1`numeric8`numeric8`alpha);
   ("B";`numeric4`numeric2`numeric4`numeric8);
   ("Z";`numeric4`numeric2`numeric4`price8`numeric8`numeric8`numeric8);
   ("t";`numeric4`numeric4`numeric4`price8`price8`price8`price8`numeric4`numeric8);
   ("Y";`numeric4`numeric2`numeric4`price8`numeric8`delta`alpha1);
   ("x";`numeric4`numeric2`alpha`alpha);
   ("q";`numeric4`numeric2`numeric4`alpha1`numeric4);
   ("W";`numeric4`numeric2`numeric4`price8`price8`price8`price8`price8`price8);
   ("V";`numeric4`numeric2`numeric4`numeric8`numeric8`numeric2);
   ("G";`numeric8)
  )
  
 
msgtypes:(!) . flip
  (("T";`timemsg);
   ("S";`endbtd);
   ("f";`futuresd);
   ("h";`optionsd);
   ("M";`combinationsd);
   ("m";`bundlessd);
   ("O";`obookstate);
   ("A";`oadd);
   ("X";`ocancel);
   ("D";`odelete);
   ("E";`oexecuted);
   ("C";`aoexecuted);
   ("e";`coexecuted);
   ("j";`ioadd);
   ("l";`ioreplace);
   ("k";`iodelete);
   ("P";`texecuted);
   ("p";`ctexecuted);
   ("B";`tcancel);
   ("Z";`equilprice);
   ("t";`adjustment);
   ("Y";`marketsettle);
   ("x";`textmessage);
   ("q";`rquote);
   ("W";`othreshold);
   ("V";`voi);
   ("G";`snapshot)
  )

/Set up table schemas

timemsg:([]second:());
endbtd:([]timestamp:();tradedate:();eventcode:());
futuresd:([]timestamp:();tradedate:();instrumid:();sym:();symname:();isin:();ex:();instrument:();cfi:();expyear:();expmonth:();pricedisp:();pricefrac:();pricemin:();lasttradingdate:();priordaysettlement:();currency:();lotsize:();maturityval:();couponrate:();paymentsperyear:();blocklotsize:();expdate:());
optionsd:([]timestamp:();tradedate:();instrumid:();sym:();symname:();isin:();ex:();instrument:();cfi:();expyear:();expmonth:();optiontype:();strike:();underlyingid:();pricedisp:();pricefrac:();pricemin:();spricedisp:();spricefrac:();spricemin:();lasttradingdate:();priordaysettlement:();volatility:();currency:();lotsize:();maturityval:();couponrate:();paymentsperyear:();blocklotsize:();expdate:();basisquotation:());
combinationsd:([]timestamp:();tradedate:();instrumid:();sym:();symname:();cfi:();pricemeth:();pricedisp:();pricefrac:();pricemin:();legs:();instrumidleg1:();sideleg1:();ratioleg1:();priceleg1:();instrumidleg2:();sideleg2:();ratioleg2:();priceleg2:();instrumidleg3:();sideleg3:();ratioleg3:();priceleg3:();instrumidleg4:();sideleg4:();ratioleg4:();priceleg4:();instrumidleg5:();sideleg5:();ratioleg5:();priceleg5:();instrumidleg6:();sideleg6:();ratioleg6:();priceleg6:());
bundlessd:([]timestamp:();tradedate:();instrumid:();sym:();symname:();cfi:();pricemeth:();pricedisp:();pricefrac:();pricemin:();legs:();instrumidleg1:();sideleg1:();ratioleg1:();priceleg1:();instrumidleg2:();sideleg2:();ratioleg2:();priceleg2:();instrumidleg3:();sideleg3:();ratioleg3:();priceleg3:();instrumidleg4:();sideleg4:();ratioleg4:();priceleg4:();instrumidleg5:();sideleg5:();ratioleg5:();priceleg5:();instrumidleg6:();sideleg6:();ratioleg6:();priceleg6:();instrumidleg7:();sideleg7:();ratioleg7:();priceleg7:();instrumidleg8:();sideleg8:();ratioleg8:();priceleg8:();instrumidleg9:();sideleg9:();ratioleg9:();priceleg9:();instrumidleg10:();sideleg10:();ratioleg10:();priceleg10:();instrumidleg11:();sideleg11:();ratioleg11:();priceleg11:();instrumidleg12:();sideleg12:();ratioleg12:();priceleg12:();instrumidleg13:();sideleg13:();ratioleg13:();priceleg13:();instrumidleg14:();sideleg14:();ratioleg14:();priceleg14:();instrumidleg15:();sideleg15:();ratioleg15:();priceleg15:();instrumidleg16:();sideleg16:();ratioleg16:();priceleg16:();instrumidleg17:();sideleg17:();ratioleg17:();priceleg17:();instrumidleg18:();sideleg18:();ratioleg18:();priceleg18:();instrumidleg19:();sideleg19:();ratioleg19:();priceleg19:();instrumidleg20:();sideleg20:();ratioleg20:();priceleg20:());
obookstate:([]timestamp:();tradedate:();instrumid:();state:());
oadd:([]timestamp:();tradedate:();instrumid:();side:();orderid:();timepriority:();size:();price:());
ocancel:([]timestamp:();tradedate:();instrumid:();side:();orderid:();size:());
odelete:([]timestamp:();tradedate:();instrumid:();side:();orderid:());
oexecuted:([]timestamp:();tradedate:();instrumid:();side:();orderid:();sizeremain:();tradetype:();tradeid:();size:();tradeprice:();combtradeid:();counterpid:());
aoexecuted:([]timestamp:();tradedate:();instrumid:();side:();orderid:();sizeremain:();tradetype:();tradeid:();size:();tradeprice:();oppositeorderid:());
coexecuted:([]timestamp:();tradedate:();instrumid:();side:();orderid:();sizeremain:();tradetype:();tradeid:();size:();tradeprice:();oppositetradeid:();oppositeside:();oppositeorderid:();combtradeid:());
ioadd:([]timestamp:();tradedate:();instrumid:();side:();orderid:();timepriority:();size:();price:());
ioreplace:([]timestamp:();tradedate:();instrumid:();side:();orderid:();timepriority:();size:();price:());
iodelete:([]timestamp:();tradedate:();instrumid:();side:();orderid:());
texecuted:([]timestamp:();tradedate:();instrumid:();tradetype:();tradeid:();size:();tradeprice:();combtradeid:();buyerid:();sellerid:());
ctexecuted:([]timestamp:();tradedate:();instrumid:();tradetype:();tradeid:();size:();tradeprice:();buyerstocklocate:();buyerside:();buyerorderid:();buyercombtradeid:();buyerid:();sellerstocklocate:();sellerside:();sellerorderid:();sellercombtradeid:();sellerid:());
tcancel:([]timestamp:();tradedate:();instrumid:();tradeid:());
equilprice:([]timestamp:();tradedate:();instrumid:();equilprice:();sizematched:();bsize:();asize:());
adjustment:([]timestamp:();tradedate:();instrumid:();opentrade:();hightrade:();lowtrade:();lasttrade:();lastvol:();totalvol:());
marketsettle:([]timestamp:();tradedate:();instrumid:();settleprice:();volatility:();delta:();settletype:());
textmessage:([]timestamp:();tradedate:();sourceid:();message:());
rquote:([]timestamp:();tradedate:();instrumid:();side:();size:());
othreshold:([]timestamp:();tradedate:();instrumid:();aotprice:();aotuprice:();aotlprice:();etrprice:();etruprice:();etrlprice:());
voi:([]timestamp:();tradedate:();instrumid:();cumulativevol:();openinterest:();voitradedate:());
snapshot:([]sequenceno:());

{[x]update seqno:`long$() from x}each tables[];                                                     /add sequence number column to tables so book is sorted by number not time

/################################ Parser ################################ 

/This section contains all the function which will parse the itch file.

setcutpoints:{[n]filebytesize:count n; 	                              	                            /Get the size of the file to determine when to finish the function
  {[n;x]length:256 sv n[x+0 1];                                                                     /First two bytes of an ITCH message represent length of that message, create a list of cut points  
    op,:x;x:x+length+2;x}[n;]/	                                                                    /The file into indivdual messages. op is a global varible
    [{x<y}[;filebytesize];0]};                                                                      /which is used for preformance reasons.

convertdata:{
  f:{[msgtyp;piece]
    {[msgtyp;x] 
    x[0],enlist _[msgoffsets[msgtyp];x[1]]} [msgtyp;]each   
    flip value exec seqno,2_'data from piece where msgtype=msgtyp}[;x]peach key msgtypes;	    /Insert these messages into the empty tables found in the config section. 
  getseqno:-1_''f;
  fconv:{[x;y]{x@ raze y}'/:[typesf types[x];raze y]}'[key msgtypes;1_''f]; 
  :fconv,''getseqno 
  };

itchinserter:{upsert'[value msgtypes;convertdata[x]]};

readfile:{read1 hsym x};

/cut file into indivdual messages then create a table with message types and data corresponding to each message. Here n is an itch file and op is a global created by the cutter function.
createdataandtypes:{[n]c:op cut n;t:([]seqno:1+til count c;msgtype:"c"$c[;2];data:c)}

saving:{[s;d;t]
  if[0=count value t;:()];
  $[`stock in cols[t];:.Q.dpft[hsym s;d;`stock;t];
  hsym[`$ "/" sv string (s),d,t,`]set .Q.en[hsym[s];value t]]
  };

savetables:{[o]
  saving[o`saveto;o`date] each tables[]};

/############################### Runtime function ###############################

init:{[o]
  n:readfile[o`itchfile];                                                                           /Read in itchfile.
  setcutpoints[n];                                                                                  /Set cut points for n as a global var called t:createdataandtypes[n];
  t:createdataandtypes[n];                                                                          /Cuts and creates a table of messagetypes and data for each message in the ITCH file.
  pieces:o[`cutsize]cut t;                                                                          /Breaks the table t created above into smaller pieces. The size of these pieces is specfied in the command line.
  itchinserter'[pieces];                                                                            /Cast the data in a useable kdb format and insert them into tables.
  if[o`save;savetables[o]];                                                                         /Saves tables on disk as a partitioned database if the user wishes to.
  };

if[p[`init];
  @[init;p;{[p;x] -1 string[.z.p]," A fatal error occurred due to ",(raze string x);if[not `noexit in key p;exit[0]]}p]; 
  -1 string[.z.p]," Script executed successfully."; 
  if[not `noexit in key p;exit[0]]];
