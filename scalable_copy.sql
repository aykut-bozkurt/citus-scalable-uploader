CREATE OR REPLACE FUNCTION scalable_copy_to_csv_internal(text, text) RETURNS void AS $$
DECLARE
        tablename text := $1;
	outputpath text := $2;
        shardports RECORD;
        filename text;
        shardname text;
BEGIN
        -- iterate on the current node's shardids for the table
        FOR shardports IN
        select * from (select shardid, nodeport from pg_dist_shard s inner join
                pg_dist_shard_placement p using (shardid) where s.logicalrelid = tablename::regclass::oid and nodeport =
                (select nodeport from pg_dist_node join pg_dist_local_group using (groupid))) table_shard_ports inner join
                pg_dist_node n using (nodeport)
        LOOP
                select into shardname (tablename || '_' || shardports.shardid);
                select into filename (outputpath || '/' || shardname || '.csv');

       		 -- copy current node's shards for the table to the corresponding csv file
		EXECUTE format('copy %s to program ''bash ~/citus-scalable-uploader/s3-uploader.sh "%s"''', shardname, filename);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scalable_copy_to_csv(text, text) RETURNS void AS $$
DECLARE
        tablename text := $1;
	outputpath text := $2;
        shard record;
        shardname text;
	res record;
	currentnodeid int;
BEGIN
	select groupid into currentnodeid from pg_dist_local_group;
	IF currentnodeid <> 0 THEN
		raise 'should only be called on coordinator!';
   	END IF;

        -- lock table to prevent any ddl on coordinator
	EXECUTE 'LOCK ' || tablename || ' IN EXCLUSIVE MODE';
	raise notice 'locked the relation % at coordinator', tablename;

	-- lock table's shards in pg_dist_shard to prevent any dml on workers
	FOR shard IN
	select shardid, nodename, nodeport from (select shardid, nodeport from pg_dist_shard s inner join
                pg_dist_shard_placement p using (shardid) where s.logicalrelid = 'dist'::regclass::oid) table_shard_ports inner join
                pg_dist_node n using (nodeport) order by shardid
        LOOP
		select into shardname (tablename || '_' || shard.shardid);
		FOR res IN
        	SELECT * from run_command_on_workers('LOCK ' || '''' || shardname || '''' || ' IN EXCLUSIVE MODE')
        	LOOP
                	raise notice 'locked the shard % at node %:%', shard.shardid, res.nodename, res.nodeport;
        	END LOOP;
        END LOOP;

	-- run scalable copy on all workers
	FOR res IN
	SELECT * from run_command_on_workers('select scalable_copy_to_csv_internal(' || '''' || tablename || '''' || ',' || '''' || outputpath || ''''|| ')')
	LOOP
		raise notice 'copied shards at node %:%', res.nodename, res.nodeport;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

