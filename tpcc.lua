#!/usr/bin/env sysbench

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("tpcc_common")
require("tpcc_run")
require("tpcc_check")

function thread_init()

   drv = sysbench.sql.driver()
   con = drv:connect()

   set_isolation_level(drv,con) 

   if drv:name() == "mysql" then 
     con:query("SET autocommit=0")
   end
  

   -- prepare statement for postgresql

   if drv:name() == "pgsql" then 
     for table_num = 1, sysbench.opt.tables
     do

       con:query(([[prepare p_new_order1_%d(int2,int2,int4) as SELECT c_discount, c_last, c_credit, w_tax 
                                                           FROM customer%d, warehouse%d
                                                          WHERE w_id = $1 
                                                            AND c_w_id = w_id 
                                                            AND c_d_id = $2 
                                                            AND c_id = $3]]):
                                                        format(table_num, table_num, table_num))

       con:query(([[prepare p_new_order2_%d(int2,int2) as SELECT d_next_o_id, d_tax 
                                          FROM district%d 
                                         WHERE d_w_id = $1 
                                           AND d_id = $2 FOR UPDATE]]):
                                        format(table_num, table_num))


       con:query(([[prepare p_new_order3_%d(int4,int2,int2) as UPDATE district%d
                  SET d_next_o_id = $1
                WHERE d_id = $2 AND d_w_id= $3]]):format(table_num, table_num))

       con:query(([[prepare p_new_order4_%d(int4,int2,int2,int4,int2,int2) as INSERT INTO orders%d
                           (o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
                    VALUES ($1,$2,$3,$4,NOW(),$5,$6)]]):
                    format(table_num, table_num))

       con:query(([[prepare p_new_order5_%d(int4,int2,int2) as INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
                    VALUES ($1,$2,$3)]]):
                   format(table_num, table_num))

       con:query(([[prepare p_new_order6_%d(int4) as SELECT i_price, i_name, i_data 
                            FROM item%d
                           WHERE i_id = $1]]):
                          format(table_num, table_num))

       for d_id = 1, DIST_PER_WARE
       do 
         con:query(([[prepare p_new_order7_%d_%s(int4,int2) as SELECT s_quantity, s_data, s_dist_%s s_dist 
                                                          FROM stock%d  
                                                         WHERE s_i_id = $1 AND s_w_id= $2 FOR UPDATE]]):
                                                         format(table_num, string.format("%02d",d_id), string.format("%02d",d_id), table_num))
       end

       con:query(([[prepare p_new_order8_%d(int2,int4,int2) as UPDATE stock%d
                        SET s_quantity =$1 
                      WHERE s_i_id = $2 
                        AND s_w_id= $3]]):
                    format(table_num, table_num))


       con:query(([[prepare p_new_order9_%d(int4,int2,int2,int2,int4,int2,int2,float8,text) as INSERT INTO order_line%d
                                 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)]]):
                          format(table_num, table_num))

       
       con:query(([[prepare p_payment1_%d(float8,int2) as UPDATE warehouse%d
                  SET w_ytd = w_ytd + $1 
                WHERE w_id = $2]]):format(table_num, table_num))
  
       con:query(([[prepare p_payment2_%d(int2) as SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name 
                                             FROM warehouse%d  
                                            WHERE w_id = $1]]):format(table_num, table_num))
       
       con:query(([[prepare p_payment3_%d(float8,int2,int2) as UPDATE district%d 
                 SET d_ytd = d_ytd + $1 
               WHERE d_w_id = $2 
                 AND d_id= $3]]):format(table_num, table_num))

       con:query(([[prepare p_payment4_%d(int2,int2) as SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name 
                                             FROM district%d
                                            WHERE d_w_id = $1 
                                              AND d_id = $2]]):format(table_num, table_num))
       
       con:query(([[prepare p_payment5_%d(int2,int2,text) as SELECT count(c_id) namecnt
                               FROM customer%d
                              WHERE c_w_id = $1 
                                AND c_d_id=$2 
                                            AND c_last=$3]]):format(table_num, table_num))
       
       con:query(([[prepare p_payment6_%d(int2,int2,text) as SELECT c_id
                FROM customer%d
               WHERE c_w_id = $1 AND c_d_id= $2 
                             AND c_last=$3 ORDER BY c_first]]):format(table_num, table_num))
       
       con:query(([[prepare p_payment7_%d(int2,int2,int4) as SELECT c_first, c_middle, c_last, c_street_1,
                                 c_street_2, c_city, c_state, c_zip, c_phone,
                                 c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
                FROM customer%d
               WHERE c_w_id = $1 
                 AND c_d_id= $2 
                 AND c_id=$3 FOR UPDATE]])
             :format(table_num, table_num))
       
       con:query(([[prepare p_payment8_%d(int2,int2,int4) as SELECT c_data
                                    FROM customer%d
                                   WHERE c_w_id = $1 
                                     AND c_d_id = $2
                                     AND c_id= $3]]):
                                  format(table_num, table_num))
       
       con:query(([[prepare p_payment9_%d(float8,float8,text,int2,int2,int4) as UPDATE customer%d
                        SET c_balance=$1, c_ytd_payment=$2, c_data=$3
                      WHERE c_w_id = $4 
                        AND c_d_id = $5
                        AND c_id = $6]])
                  :format(table_num, table_num  ))

       con:query(([[prepare p_payment10_%d(float8,float8,int2,int2,int4) as UPDATE customer%d
                        SET c_balance=$1, c_ytd_payment=$2
                      WHERE c_w_id = $3 
                        AND c_d_id = $4
                        AND c_id = $5]])
                  :format(table_num, table_num  ))

       con:query(([[prepare p_payment11_%d(int2,int2,int4,int2,int2,float8,text) as INSERT INTO history%d
                           (h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)
                    VALUES ($1,$2,$3,$4,$5,NOW(),$6,$7)]])
            :format(table_num, table_num))


       con:query(([[prepare p_orderstatus1_%d(int2,int2,text) as SELECT count(c_id) namecnt
                                     FROM customer%d
                                    WHERE c_w_id = $1 
                                      AND c_d_id= $2 
                                      AND c_last=$3]]):
                                  format(table_num, table_num))

       
       con:query(([[prepare p_orderstatus2_%d(int2,int2,text) as SELECT c_balance, c_first, c_middle, c_id
                            FROM customer%d
                       WHERE c_w_id = $1 
                         AND c_d_id= $2 
                             AND c_last=$3 ORDER BY c_first]]):
                                  format(table_num, table_num))
       
       con:query(([[prepare p_orderstatus3_%d(int2,int2,int4) as SELECT c_balance, c_first, c_middle, c_last
                                      FROM customer%d
                                 WHERE c_w_id = $1 
                                   AND c_d_id= $2
                                       AND c_id=$3]])
                                  :format(table_num, table_num ))
       
       con:query(([[prepare p_orderstatus4_%d(int2,int2,int4) as  SELECT o_id, o_carrier_id, o_entry_d
                                FROM orders%d 
                               WHERE o_w_id = $1 
                                 AND o_d_id = $2 
                                 AND o_c_id = $3 
                                  ORDER BY o_id DESC]]):
                             format(table_num, table_num ))
       
       con:query(([[prepare p_orderstatus5_%d(int2,int2,int4) as SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line%d WHERE ol_w_id = $1 AND ol_d_id = $2  AND ol_o_id = $3]])
                  :format(table_num, table_num ))
       

       con:query(([[prepare p_delivery1_%d(int2,int2) as SELECT no_o_id 
                                     FROM new_orders%d 
                                    WHERE no_d_id = $1 
                                      AND no_w_id = $2 
                                      ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE]])
                                   :format(table_num, table_num))
       
       con:query(([[prepare p_delivery2_%d(int4,int2,int2) as DELETE FROM new_orders%d 
                           WHERE no_o_id = $1  
                             AND no_d_id = $2  
                             AND no_w_id = $3]]) 
                            :format(table_num, table_num)) 

       con:query(([[prepare p_delivery3_%d(int4,int2,int2) as  SELECT o_c_id
                                    FROM orders%d 
                                   WHERE o_id = $1 
                                     AND o_d_id = $2 
                                     AND o_w_id = $3]])
                                  :format(table_num, table_num))

        con:query(([[prepare p_delivery4_%d(int2,int4,int2,int2) as  UPDATE orders%d 
                        SET o_carrier_id = $1 
                      WHERE o_id = $2  
                        AND o_d_id = $3  
                        AND o_w_id = $4]]) 
                      :format(table_num, table_num)) 

        con:query(([[prepare p_delivery5_%d(int4,int2,int2) as  UPDATE order_line%d 
                        SET ol_delivery_d = NOW()
                      WHERE ol_o_id = $1 
                        AND ol_d_id = $2 
                        AND ol_w_id = $3]])
                      :format(table_num, table_num))

       con:query(([[prepare p_delivery6_%d(int4,int2,int2) as SELECT SUM(ol_amount) sm
                                          FROM order_line%d 
                                         WHERE ol_o_id = $1 
                                           AND ol_d_id = $2 
                                           AND ol_w_id = $3]])
                                      :format(table_num, table_num))


        con:query(([[prepare p_delivery7_%d(float8,int4,int2,int2) as UPDATE customer%d 
                        SET c_balance = c_balance + $1,
                            c_delivery_cnt = c_delivery_cnt + 1
                      WHERE c_id = $2 
                        AND c_d_id = $3 
                        AND c_w_id = $4]])
                      :format(table_num, table_num))



       con:query(([[prepare p_stocklevel1_%d(int2,int2) as  SELECT d_next_o_id 
                                     FROM district%d
                                    WHERE d_id = $1 AND d_w_id= $2]])
                          :format( table_num, table_num ))


       con:query(([[prepare p_stocklevel2_%d(int2,int2,int4,int4,int2,int2) as SELECT COUNT(DISTINCT (s_i_id))
                        FROM order_line%d, stock%d
                       WHERE ol_w_id = $1 
                         AND ol_d_id = $2 
                         AND ol_o_id < $3 
                         AND ol_o_id >= $4 
                         AND s_w_id= $5 
                         AND s_i_id=ol_i_id 
                         AND s_quantity < $6 ]])
        :format(table_num, table_num, table_num ))

       con:query(([[prepare p_stocklevel3_%d(int2,int2,int4,int4) as SELECT DISTINCT ol_i_id FROM order_line%d
               WHERE ol_w_id = $1 AND ol_d_id = $2 
                 AND ol_o_id < $3 AND ol_o_id >= $4]])
                :format(table_num, table_num ))

       con:query(([[prepare p_stocklevel4_%d(int2,int4,int2) as SELECT count(*) FROM stock%d
                   WHERE s_w_id = $1 AND s_i_id = $2 
                   AND s_quantity < $3]])
                :format(table_num, table_num) )




       con:query(([[prepare p_purge1_%d(int2,int2) as SELECT min(no_o_id) mo
                                     FROM new_orders%d 
                                    WHERE no_w_id = $1 AND no_d_id = $2]])
                                   :format(table_num, table_num))

       con:query(([[prepare p_purge2_%d(int2,int2,int4) as SELECT o_id FROM orders%d o, 
             (SELECT o_c_id,o_w_id,o_d_id,count(distinct o_id) FROM orders%d WHERE o_w_id=$1 AND o_d_id=$2 AND o_id > 2100 
             AND o_id < $3 GROUP BY o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t 
             WHERE t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1 ]])
                                   :format(table_num, table_num, table_num))

       con:query(([[prepare p_purge3_%d(int2,int2,int4) as DELETE FROM order_line%d where ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3]])
                            :format(table_num, table_num))

       con:query(([[prepare p_purge4_%d(int2,int2,int4) as DELETE FROM orders%d where o_w_id=$1 AND o_d_id=$2 and o_id=$3]])
                            :format(table_num, table_num))

       con:query(([[prepare p_purge5_%d(int2,int2) as DELETE FROM history%d where ctid = any (array(select ctid from history%d where h_w_id=$1 AND h_d_id=$2 LIMIT 10))]])
                            :format(table_num, table_num, table_num ))

     end
   end 
end

function event()
  -- print( NURand (1023,1,3000))
  local max_trx =  sysbench.opt.enable_purge == "yes" and 24 or 23
  local trx_type = sysbench.rand.uniform(1,max_trx)
  if trx_type <= 10 then
    trx="new_order"
  elseif trx_type <= 20 then
    trx="payment"
  elseif trx_type <= 21 then
    trx="orderstatus"
  elseif trx_type <= 22 then
    trx="delivery"
  elseif trx_type <= 23 then
    trx="stocklevel"
  elseif trx_type <= 24 then
    trx="purge"
  end

-- Execute transaction
   _G[trx]()

end

function sysbench.hooks.before_restart_event(err)
  con:query("ROLLBACK")
end

function sysbench.hooks.report_intermediate(stat)
-- --   print("my stat: ", val)
   if  sysbench.opt.report_csv == "yes" then
        sysbench.report_csv(stat)
   else
        sysbench.report_default(stat)
   end
end


-- vim:ts=4 ss=4 sw=4 expandtab
