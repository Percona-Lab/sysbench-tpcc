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
   con:query("SET autocommit=0")
   
end

function event()
  -- print( NURand (1023,1,3000))
  local trx_type = sysbench.rand.uniform(1,23)
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
  end

-- Repeat transaction execution until success
  while not pcall(function () _G[trx]() end ) do 
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    con:query("ROLLBACK")
  end

end

function sysbench.hooks.report_intermediate(stat)
-- --   print("my stat: ", val)
   sysbench.report_csv(stat)
end


-- vim:ts=4 ss=4 sw=4 expandtab
