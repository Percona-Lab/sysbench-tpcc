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

   if drv:name() == "mysql"
   then
        if sysbench.opt.trx_level == "RR" then
            con:query("SET SESSION transaction_isolation='REPEATABLE-READ'")
        elseif sysbench.opt.trx_level == "RC" then
            con:query("SET SESSION transaction_isolation='READ-COMMITTED'")
        elseif sysbench.opt.trx_level == "SER" then
            con:query("SET SESSION transaction_isolation='SERIALIZABLE'")
        end
   end
end

function event()
  -- print( NURand (1023,1,3000))
  local trx = sysbench.rand.uniform(1,23)
  if trx <= 10 then
    new_order()
  elseif trx <= 20 then
    payment()
  elseif trx <= 21 then
 --   print("order status")
    orderstatus()
  elseif trx <= 22 then
 --   print("delivery")
    delivery()
  elseif trx <= 23 then
 --   print("delivery")
    stocklevel()
  end
end

-- vim:ts=4 ss=4 sw=4 expandtab
