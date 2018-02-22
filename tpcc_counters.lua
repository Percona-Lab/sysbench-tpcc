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

function sysbench.report_custom(stat)
   local seconds = stat.time_interval
   print(string.format("[ %.0fs ] thds: %u tps: %4.2f qps: %4.2f " ..
                          "(r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%u%%): %4.2f " ..
                          "err/s %4.2f reconn/s: %4.2f no: %4.2f pt: %4.2f os: %4.2f dl: %4.2f sl: %4.2f",
                       stat.time_total,
                       stat.threads_running,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads / seconds,
                       stat.writes / seconds,
                       stat.other / seconds,
                       sysbench.opt.percentile,
                       stat.latency_pct * 1000,
                       stat.errors / seconds,
                       stat.reconnects / seconds,
                       stat.cnt1 / seconds,
                       stat.cnt2 / seconds,                       
                       stat.cnt3 / seconds,
                       stat.cnt4 / seconds,                       
                       stat.cnt5 / seconds
   ))
end

ffi = require("ffi")

ffi.cdef[[
void sb_counter_inc(int, sb_counter_type);
]]

sysbench.hooks.report_intermediate = sysbench.report_custom

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   set_isolation_level(drv,con) 
   
end

function event()
  -- print( NURand (1023,1,3000))
  local trx_type = sysbench.rand.uniform(1,23)
  if trx_type <= 10 then
--    print("new_order")
    trx=new_order
    counter=ffi.C.SB_CNT_1
  elseif trx_type <= 20 then
--   print("payment")
    trx=payment
    counter=ffi.C.SB_CNT_2
  elseif trx_type <= 21 then
--   print("order status")
    trx=orderstatus
    counter=ffi.C.SB_CNT_3
  elseif trx_type <= 22 then
--   print("delivery")
    trx=delivery
    counter=ffi.C.SB_CNT_4
  elseif trx_type <= 23 then
--    print("stock")
    trx=stocklevel
    counter=ffi.C.SB_CNT_5
  end

-- Repeat transaction execution until success
  while not pcall(function () trx() end ) do end
  ffi.C.sb_counter_inc(sysbench.tid, counter)


end

-- vim:ts=4 ss=4 sw=4 expandtab
function sysbench.hooks.report_cumulative(stat)
    local seconds = stat.time_interval
    print(string.format([[
{
    "errors": %4.0f,
    "events": %4.0f,
    "latency_avg": %4.10f,
    "latency_max": %4.10f,
    "latency_min": %4.10f,
    "latency_pct": %4.10f,
    "latency_sum": %4.10f,
    "other": %4.0f,
    "reads": %4.0f,
    "reconnects": %4.0f,
    "threads_running": %4.0f,
    "time_interval": %4.10f,
    "time_total": %4.10f,
    "writes": %4.0f,
    "new_order": %4.0f,
    "payment": %4.0f,    
    "order_status": %4.0f,
    "delivery": %4.0f,
    "stock_level": %4.0f
}
]],
    stat.errors,
    stat.events,
    stat.latency_avg,
    stat.latency_max,
    stat.latency_min,
    stat.latency_pct,
    stat.latency_sum,
    stat.other,
    stat.reads,
    stat.reconnects,
    stat.threads_running,
    stat.time_interval,
    stat.time_total,
    stat.writes,
    stat.cnt1,
    stat.cnt2,    
    stat.cnt3,    
    stat.cnt4,
    stat.cnt5
    ))
end

