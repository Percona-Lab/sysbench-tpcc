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


--
-- produce the id of a valid warehouse other than home_ware
-- (assuming there is one)
--
function other_ware (home_ware)
    local tmp

    if sysbench.opt.scale == 1 then return home_ware end
    repeat
       tmp = sysbench.rand.uniform(1, sysbench.opt.scale)
    until tmp == home_ware
    return tmp
end

function new_order()

-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)

    local ol_cnt = sysbench.rand.uniform(5, 15);
    local rbk = sysbench.rand.uniform(1, 100);
    local itemid = {}
    local supware = {}
    local qty = {}
    local all_local = 1

    for i = 1, ol_cnt
    do
        itemid[i] = NURand(8191, 1, MAXITEMS)
        if ((i == ol_cnt - 1) and (rbk == 1))
	then
            itemid[i] = -1
        end
        if sysbench.rand.uniform(1, 100) ~= 1
	then
            supware[i] = w_id
        else 
            supware[i] = other_ware(w_id)
            all_local = 0
        end
        qty[i] = sysbench.rand.uniform(1, 10)
   end


--  SELECT c_discount, c_last, c_credit, w_tax
--  INTO :c_discount, :c_last, :c_credit, :w_tax
--  FROM customer, warehouse
--  WHERE w_id = :w_id 
--  AND c_w_id = w_id 
--  AND c_d_id = :d_id 
--  AND c_id = :c_id;

  con:query("BEGIN")
  rs = con:query([[SELECT c_discount, c_last, c_credit, w_tax 
                   FROM customer]]..table_num..", warehouse"..table_num..
                 " WHERE w_id = ".. w_id ..
                 " AND c_w_id = w_id AND c_d_id = "..d_id.."  AND c_id = "..c_id)

  local c_discount
  local c_last
  local c_credit
  local w_tax
  for i = 1, rs.nrows do
    row = rs:fetch_row()
    c_discount = row[1]
    c_last = row[2]
    c_credit = row[3]
    w_tax = row[4]
    -- print(row[1], row[2], row[3], row[4])
  end
  --rs.free()

--        SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
--                FROM district
--                WHERE d_id = :d_id
--                AND d_w_id = :w_id
--                FOR UPDATE
  rs = con:query("SELECT d_next_o_id, d_tax FROM district".. table_num ..
                 " WHERE d_w_id = ".. w_id .." AND d_id = ".. d_id .." FOR UPDATE")

  local d_next_o_id
  local d_tax
  for i = 1, rs.nrows do
    row = rs:fetch_row()
    d_next_o_id = row[1]
    d_tax = row[2]
    -- print(row[1], row[2])
  end
  --rs.free()
 
-- UPDATE district SET d_next_o_id = :d_next_o_id + 1
--                WHERE d_id = :d_id 
--                AND d_w_id = :w_id;

  con:query("UPDATE district".. table_num ..
            " SET d_next_o_id = ".. (d_next_o_id + 1) ..
            " WHERE d_id = ".. d_id .." AND d_w_id=".. w_id)

--INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
--                                    o_entry_d, o_ol_cnt, o_all_local)
--                VALUES(:o_id, :d_id, :w_id, :c_id, 
--                       :datetime,
--                       :o_ol_cnt, :o_all_local);

  con:query("INSERT INTO orders".. table_num ..
            "(o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)"..
            string.format(" VALUES (%d,%d,%d,%d,NOW(),%d,%d)",
            d_next_o_id, d_id, w_id, c_id, ol_cnt, all_local))

-- INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
--    VALUES (:o_id,:d_id,:w_id); */

  con:query("INSERT INTO new_orders".. table_num ..
            "(no_o_id, no_d_id, no_w_id)"..
            string.format(" VALUES (%d,%d,%d)",
            d_next_o_id, d_id, w_id))

  for ol_number=1, ol_cnt do
	local ol_supply_w_id = supware[ol_number]
	local ol_i_id = itemid[ol_number]
	local ol_quantity = qty[ol_number]

-- SELECT i_price, i_name, i_data
--	INTO :i_price, :i_name, :i_data
--	FROM item
--	WHERE i_id = :ol_i_id;*/

	  rs = con:query("SELECT i_price, i_name, i_data FROM item".. table_num ..
			 " WHERE i_id = ".. ol_i_id)

	  local i_price
	  local i_name
	  local i_data

	  if rs.nrows == 0 then
        --print("ROLLBACK")
        con:query("ROLLBACK")
	    return	
      end

	  for i = 1, rs.nrows do
	    row = rs:fetch_row()
	    i_price = row[1]
	    i_name = row[2]
	    i_data = row[3]
	    -- print(row[1], row[2], row[3])
	  end
        
-- SELECT s_quantity, s_data, s_dist_01, s_dist_02,
--		s_dist_03, s_dist_04, s_dist_05, s_dist_06,
--		s_dist_07, s_dist_08, s_dist_09, s_dist_10
--	INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
--	     :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
--	     :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
--	FROM stock
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id
--	FOR UPDATE;*/

	  rs = con:query("SELECT s_quantity, s_data, s_dist_"..string.format("%02d",d_id) ..
                         " s_dist FROM stock".. table_num ..
			 " WHERE s_i_id = ".. ol_i_id .. " AND s_w_id=".. ol_supply_w_id ..
                         " FOR UPDATE")
          local s_quantity 
          local s_data 
          local ol_dist_info
	  for i = 1, rs.nrows do
	    row = rs:fetch_row()
	    s_quantity = tonumber(row[1])
	    s_data = row[2]
	    ol_dist_info = row[3]
	    -- print(row[1], row[2], row[3])
	  end

	if s_quantity > ol_quantity then
		s_quantity = s_quantity - ol_quantity
	else
		s_quantity = s_quantity - ol_quantity + 91
	end

-- UPDATE stock SET s_quantity = :s_quantity
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id;*/

	  con:query("UPDATE stock".. table_num ..
		    " SET s_quantity = ".. s_quantity ..
		    " WHERE s_i_id = ".. ol_i_id .." AND s_w_id=".. ol_supply_w_id)
   
	  ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

-- INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
--				 ol_number, ol_i_id, 
--				 ol_supply_w_id, ol_quantity, 
--				 ol_amount, ol_dist_info)
--	VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
--		:ol_supply_w_id, :ol_quantity, :ol_amount,
--		:ol_dist_info);

	  con:query("INSERT INTO order_line".. table_num ..
	            " (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)"..
		    " VALUES "..
                    string.format("(%d,%d,%d,%d,%d,%d,%d,%d,'%s')",
                    d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                   )

  end
  con:query("COMMIT")

end

function payment()
-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local h_amount = sysbench.rand.uniform(1,5000)
    local byname
    local c_w_id
    local c_d_id
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    if sysbench.rand.uniform(1, 100) <= 85 then
        c_w_id = w_id
        c_d_id = d_id
    else
        c_w_id = other_ware(w_id)
        c_d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    end

--  UPDATE warehouse SET w_ytd = w_ytd + :h_amount
--  WHERE w_id =:w_id

  con:query("BEGIN")

  con:query("UPDATE warehouse".. table_num ..
	    " SET w_ytd = w_ytd + ".. h_amount ..
	    " WHERE w_id = ".. w_id)

-- SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
--		w_name
--		INTO :w_street_1, :w_street_2, :w_city, :w_state,
--			:w_zip, :w_name
--		FROM warehouse
--		WHERE w_id = :w_id;*/

  rs = con:query([[SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name 
                   FROM warehouse]]..table_num..
                 " WHERE w_id = ".. w_id)

  local w_street_1
  local w_street_2
  local w_city
  local w_state
  local w_zip
  local w_name
  for i = 1, rs.nrows do
    row = rs:fetch_row()
    w_street_1 = row[1]
    w_street_2 = row[2]
    w_city = row[3]
    w_state = row[4]
    w_zip = row[5]
    w_name = row[6]
    -- print(row[1], row[2], row[3], row[4])
  end

-- UPDATE district SET d_ytd = d_ytd + :h_amount
--		WHERE d_w_id = :w_id 
--		AND d_id = :d_id;*/

  con:query("UPDATE district".. table_num ..
	    " SET d_ytd = d_ytd + ".. h_amount ..
	    " WHERE d_w_id = ".. w_id .. " AND d_id="..d_id)

  rs = con:query([[SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name 
                   FROM district]]..table_num..
                 " WHERE d_w_id = ".. w_id.. " AND d_id="..d_id)

  local d_street_1
  local d_street_2
  local d_city
  local d_state
  local d_zip
  local d_name
  for i = 1, rs.nrows do
    row = rs:fetch_row()
    d_street_1 = row[1]
    d_street_2 = row[2]
    d_city = row[3]
    d_state = row[4]
    d_zip = row[5]
    d_name = row[6]
    -- print(row[1], row[2], row[3], row[4])
  end


  if byname == 1 then

-- SELECT count(c_id) 
--	FROM customer
--	WHERE c_w_id = :c_w_id
--	AND c_d_id = :c_d_id
--	AND c_last = :c_last;*/
	rs = con:query([[SELECT count(c_id) namecnt
			   FROM customer]]..table_num..
			 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
                         " AND c_last='"..c_last.."'")
	local namecnt
	for i = 1, rs.nrows do
		row = rs:fetch_row()
		namecnt = row[1]
	end
  
--		SELECT c_id
--		FROM customer
--		WHERE c_w_id = :c_w_id 
--		AND c_d_id = :c_d_id 
--		AND c_last = :c_last
--		ORDER BY c_first;

	rs = con:query([[SELECT c_id
			   FROM customer]]..table_num..
			 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
                         " AND c_last='"..c_last.."'")

	if namecnt % 2 == 0 then
		namecnt = namecnt + 1
	end
	for i = 1,  (namecnt % 2 ) + 1 do
		row = rs:fetch_row()
		c_id = row[1]
	end
  end -- byname

-- SELECT c_first, c_middle, c_last, c_street_1,
--		c_street_2, c_city, c_state, c_zip, c_phone,
--		c_credit, c_credit_lim, c_discount, c_balance,
--		c_since
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
--	AND c_id = :c_id
--	FOR UPDATE;

	rs = con:query([[SELECT c_first, c_middle, c_last, c_street_1,
                         c_street_2, c_city, c_state, c_zip, c_phone,
                         c_credit, c_credit_lim, c_discount, c_balance, c_since
			   FROM customer]]..table_num..
			 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
			 " AND c_id="..c_id.." FOR UPDATE")
  local c_first
  local c_middle
  local c_last
  local c_street_1
  local c_street_2
  local c_city
  local c_state
  local c_zip
  local c_phone
  local c_credit
  local c_credit_lim
  local c_discount
  local c_balance
  local c_since
  for i = 1, rs.nrows do
	row = rs:fetch_row()
	c_first = row[1]
	c_middle = row[2]
	c_last = row[3]
	c_street_1 = row[4]
	c_street_2 = row[5]
	c_city = row[6]
	c_state = row[7]
	c_zip = row[8]
	c_phone = row[9]
	c_credit = row[10]
	c_credit_lim = row[11]
	c_discount = row[12]
	c_balance = row[13]
	c_since = row[14]
    -- print(row[1], row[2], row[3], row[4])
  end

  c_balance = tonumber(c_balance) - h_amount

    if c_credit == "BC" then
-- SELECT c_data 
--	INTO :c_data
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
-- 	AND c_id = :c_id; */

        rs = con:query([[SELECT c_data
                   FROM customer]]..table_num..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
                             " AND c_id="..c_id)
        local c_data
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            c_data = row[1]
        end

        local c_new_data=string.sub(string.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s",
                c_id, c_d_id, c_w_id, d_id, w_id, h_amount, os.time(), c_data), 1, 500);

    --		UPDATE customer
    --			SET c_balance = :c_balance, c_data = :c_new_data
    --			WHERE c_w_id = :c_w_id 
    --			AND c_d_id = :c_d_id 
    --			AND c_id = :c_id
        con:query("UPDATE customer"..table_num..
                 " SET c_balance="..c_balance..", c_data='"..c_new_data.."'"..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
                             " AND c_id="..c_id)
    else
        con:query("UPDATE customer"..table_num..
                 " SET c_balance="..c_balance..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..c_d_id ..
                             " AND c_id="..c_id)

    end

--	INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
--			                   h_w_id, h_date, h_amount, h_data)
--	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
--		               :w_id, 
--			       :datetime,
--			       :h_amount, :h_data);*/
			       
  con:query("INSERT INTO history".. table_num ..
            "(h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)"..
            string.format(" VALUES (%d,%d,%d,%d,%d,NOW(),%d,'%s')",
            c_d_id, c_w_id, c_id, d_id,  w_id, h_amount, string.format("%10s %10s    ",w_name,d_name)))
con:query("COMMIT")
end

function orderstatus()

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local byname
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    local c_balance
    local c_first
    local c_middle
    con:query("BEGIN")

    if byname == 1 then
--    /*EXEC_SQL SELECT count(c_id)
--            FROM customer
--        WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--            AND c_last = :c_last;*/

        rs = con:query([[SELECT count(c_id) namecnt
                   FROM customer]]..table_num..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..d_id ..
                             " AND c_last='"..c_last.."'")
        local namecnt
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            namecnt = row[1]
        end

--            SELECT c_balance, c_first, c_middle, c_last
--            FROM customer
--            WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--        AND c_last = :c_last
--        ORDER BY c_first;

        rs = con:query([[SELECT c_balance, c_first, c_middle, c_last
                   FROM customer]]..table_num..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..d_id ..
                             " AND c_last='"..c_last.."'"..
                 "ORDER BY c_first")

        if namecnt % 2 == 0 then
            namecnt = namecnt + 1
        end
        for i = 1,  (namecnt % 2 ) + 1 do
            row = rs:fetch_row()
            c_balance = row[1]
            c_first = row[2]
            c_middle = row[3]
            c_last = row[4]
        end
    else
--		SELECT c_balance, c_first, c_middle, c_last
--		        FROM customer
--		        WHERE c_w_id = :c_w_id
--			AND c_d_id = :c_d_id
--			AND c_id = :c_id;*/
        rs = con:query([[SELECT c_balance, c_first, c_middle, c_last
                   FROM customer]]..table_num..
                 " WHERE c_w_id = ".. w_id.. " AND c_d_id="..d_id ..
                             " AND c_id="..c_id)
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            c_balance = row[1]
            c_first = row[2]
            c_middle = row[3]
            c_last = row[4]
        end
    
    end
-- SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)

    rs = con:query(string.format([[SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) 
                  FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d AND o_id = 
                  (SELECT MAX(o_id) FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d)]],
                  table_num, w_id, d_id, c_id, table_num, w_id, d_id, c_id))
    local o_id
 
    for i = 1, rs.nrows do
        row = rs:fetch_row()
        o_id = row[1]
    end

--		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
--                       ol_delivery_d
--		FROM order_line
--	        WHERE ol_w_id = :c_w_id
--		AND ol_d_id = :c_d_id
--		AND ol_o_id = :o_id;*/
    rs = con:query(string.format([[SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line%d WHERE ol_w_id = %d AND ol_d_id = %d  AND ol_o_id = %d]],
                  table_num, w_id, d_id, d_id, o_id))
    for i = 1,  rs.nrows do
        row = rs:fetch_row()
        local ol_i_id = row[1]
        local ol_supply_w_id = row[2]
        local ol_quantity = row[3]
        local ol_amount = row[4]
        local ol_delivery_d = row[5]
    end
    con:query("COMMIT")

end

function delivery()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local o_carrier_id = sysbench.rand.uniform(1, 10)

    con:query("BEGIN")
	for d_id = 1, DIST_PER_WARE do

--	SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
--		                FROM new_orders
--		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
		                
        rs = con:query(string.format([[SELECT COALESCE(MIN(no_o_id),0) no_o_id
                FROM new_orders%d WHERE no_d_id = %d AND no_w_id = %d]],
                      table_num, d_id, w_id))
        local no_o_id
        for i = 1,  rs.nrows do
            row = rs:fetch_row()
            no_o_id = row[1]
        end

		if tonumber(no_o_id) == 0 then goto continue end

--		DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
--		  AND no_w_id = :w_id;*/

        con:query(string.format([[DELETE FROM new_orders%d
                WHERE no_o_id = %d AND no_d_id = %d  AND no_w_id = %d]],
                      table_num, no_o_id, d_id, w_id))

--  SELECT o_c_id INTO :c_id FROM orders
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id
--				AND o_w_id = :w_id;*/

        rs = con:query(string.format([[SELECT o_c_id
                FROM orders%d WHERE o_id = %d AND o_d_id = %d AND o_w_id = %d]],
                      table_num, no_o_id, d_id, w_id))

        local o_c_id
        for i = 1,  rs.nrows do
            row = rs:fetch_row()
            o_c_id = row[1]
        end

--	 UPDATE orders SET o_carrier_id = :o_carrier_id
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
--				o_w_id = :w_id;*/

        con:query(string.format([[UPDATE orders%d SET o_carrier_id = %d
                WHERE o_id = %d AND o_d_id = %d AND o_w_id = %d]],
                      table_num, o_carrier_id, no_o_id, d_id, w_id))

--   UPDATE order_line
--		                SET ol_delivery_d = :datetime
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
--				ol_w_id = :w_id;*/
        con:query(string.format([[UPDATE order_line%d SET ol_delivery_d = NOW()
                WHERE ol_o_id = %d AND ol_d_id = %d AND ol_w_id = %d]],
                      table_num, no_o_id, d_id, w_id))

--	 SELECT SUM(ol_amount) INTO :ol_total
--		                FROM order_line
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
--				AND ol_w_id = :w_id;*/

        rs = con:query(string.format([[SELECT SUM(ol_amount) sm
                FROM order_line%d WHERE ol_o_id = %d AND ol_d_id = %d AND ol_w_id = %d]],
                      table_num, no_o_id, d_id, w_id))

        local sm_ol_amount
        for i = 1,  rs.nrows do
            row = rs:fetch_row()
            sm_ol_amount = row[1]
        end

--	UPDATE customer SET c_balance = c_balance + :ol_total ,
--		                             c_delivery_cnt = c_delivery_cnt + 1
--		                WHERE c_id = :c_id AND c_d_id = :d_id AND
--				c_w_id = :w_id;*/
        con:query(string.format([[UPDATE customer%d SET c_balance = c_balance + %d,
                c_delivery_cnt = c_delivery_cnt + 1
                WHERE c_id = %d AND c_d_id = %d AND  c_w_id = %d]],
                      table_num, sm_ol_amount, o_c_id, d_id, w_id))

        ::continue::
    end
    con:query("COMMIT")

end

function stocklevel()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local level = sysbench.rand.uniform(10, 20)

    con:query("BEGIN")

--	/*EXEC_SQL SELECT d_next_o_id
--	                FROM district
--	                WHERE d_id = :d_id
--			AND d_w_id = :w_id;*/

    rs = con:query([[SELECT d_next_o_id FROM district]]..table_num..
             " WHERE d_id = ".. d_id.. " AND d_w_id="..w_id)

    local d_next_o_id
    for i = 1, rs.nrows do
        row = rs:fetch_row()
        d_next_o_id = row[1]
    end

--	                SELECT DISTINCT ol_i_id
--	                FROM order_line
--	                WHERE ol_w_id = :w_id
--			AND ol_d_id = :d_id
--			AND ol_o_id < :d_next_o_id
--			AND ol_o_id >= (:d_next_o_id - 20);

    rs = con:query([[SELECT DISTINCT ol_i_id FROM order_line]]..table_num..
             " WHERE ol_w_id = "..w_id.." AND ol_d_id = "..d_id..
             " AND ol_o_id < "..d_next_o_id.." AND ol_o_id >= "..(d_next_o_id - 20))

    local ol_i_id
    for i = 1, rs.nrows do
        row = rs:fetch_row()
        ol_i_id = row[1]


--	 SELECT count(*) INTO :i_count
--			FROM stock
--			WHERE s_w_id = :w_id
--		        AND s_i_id = :ol_i_id
--			AND s_quantity < :level;*/

        rs1 = con:query([[SELECT count(*) FROM stock]]..table_num..
                 " WHERE s_w_id = "..w_id.." AND s_i_id = "..ol_i_id..
                 " AND s_quantity < "..level)
        local cnt
        for i = 1, rs1.nrows do
            row1 = rs1:fetch_row()
            cnt = row1[1]
        end

    end

    con:query("COMMIT")

end

-- vim:ts=4 ss=4 sw=4 expandtab
