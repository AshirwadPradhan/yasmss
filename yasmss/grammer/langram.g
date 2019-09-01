v_qu : SELECT * FROM <table> INNER JOIN <table> ON <o_condition> WHERE <w_condition>
u_qu : SELECT <columns> <comma> <func>(<column>) FROM <table> GROUP BY <columns> HAVING <func>(<column>) <gt> [INT]
<table> : [A-Z, a-z]
<o_condition> : <table>.[A-Z, a-z] = <table>.[A-Z, a-z]
<w_condition> : [A-Z, a-z] <op> [INT]
<op> : [< , > , <= , >= , !=]
<columns> : [A-Z, a-z, <comma>]
<column> : [A-Z, a-z]
<comma> : ,
<func> : [COUNT, MAX, MIN, SUM]
<gt> : >