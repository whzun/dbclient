package org.whz.hiveclient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whz.base.HiveAccessor;
import org.whz.base.Utils;

public class HiveJdbcClient {
    private static final Logger log = LoggerFactory.getLogger(HiveJdbcClient.class);

    private BufferedReader fileSource = null;
    private BufferedReader cnslSource = null;
    private Properties options = null;
    private static HiveJdbcClient inst = null;

    private static final String OPT_DRIVER = "driverName";
    private static final String OPT_URL = "url";
    private static final String OPT_USER = "user";
    private static final String OPT_PWD = "password";
    private static final String OPT_FILE = "file";
    private static final String OPT_SQL = "sql";

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run()
            {
                if(inst!=null && inst.fileSource!=null) {
                    try {
                        inst.fileSource.close();
                    } catch (IOException e) {
                    }

                    try {
                        inst.cnslSource.close();
                    } catch (IOException e) {
                    }
                }
            }
        });

        Properties opts = parseOptions(args);
        if (opts == null) return;
        HiveAccessor.initDataSource(opts);

        inst = new HiveJdbcClient(opts);
        inst.execute();
    }

    private static Options cmdOptions = initCliOptions();
    private static Options initCliOptions()
    {
	    Options options = new Options();
	    options.addOption("h", "help", false, "Print this usage");
	    options.addOption("i", "ip", true, "Hive server ip address");
	    options.addOption("P", "port", true, "Server port number, default \"10000\"");
	    options.addOption("x","path",true,"Hive path, default \"\"");
	    options.addOption("u", "user", true, "User name");
	    options.addOption("p", "password", true, "Password");
	    options.addOption("d", "driver", true, "Driver class name, default \"org.apache.hive.jdbc.HiveDriver\"");
	    options.addOption("f", "file", true, "SQL file name");
	    options.addOption("s", "sql", true, "SQL script, if \"f\" is specified this option will be ignored");
	    return options;
    }

    private static Properties parseOptions(String[] args)
    {

		Properties p = new Properties();
	    try {
			CommandLineParser parser = new PosixParser();
			CommandLine cmd = parser.parse(cmdOptions, args);

			if (cmd.hasOption("h") || cmd.getOptions()==null || cmd.getOptions().length==0) {
			    printHelp();
			    return null;
			}

			String driver = cmd.getOptionValue("d","org.apache.hive.jdbc.HiveDriver");
			String ip = cmd.getOptionValue("i","127.0.0.1");
			String port = cmd.getOptionValue("P","10000");
			String path = cmd.getOptionValue("x","");
			String user = cmd.getOptionValue("u","");
			String pwd= cmd.getOptionValue("p","");
			String sqlfile= cmd.getOptionValue("f");
			String sql= cmd.getOptionValue("s");

			p.put(OPT_DRIVER, driver);
			p.put(OPT_URL, String.format("jdbc:hive2://%s:%s/%s",ip,port,path));
			p.put(OPT_USER, user);
			p.put(OPT_PWD, pwd);
			if(sqlfile!=null) p.put(OPT_FILE, sqlfile);
			if(sql!=null)p.put(OPT_SQL, sql);

		} catch (ParseException e) {
			log.error("Options parse failed with {}",e.getMessage());
		}
		return p;


    }

    private static void printHelp() {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("Hive Client", cmdOptions );
	    System.out.println();
    }

    public HiveJdbcClient(Properties opts)
    {
    	this.options = opts;

        String fname = this.options.getProperty(OPT_FILE);

        if(fname!=null && fname.length()>0) {

            File f = new File(fname);
            if(f.exists() && f.isFile()) {
                try {
                    this.fileSource = new BufferedReader(new FileReader(f));
                } catch (FileNotFoundException e) {
                    log.error(e.getMessage());
                }
            } else {
            	log.info("file \"{}\" not exist", fname);
            }
        } else {
        	if(this.options.get(OPT_SQL)!=null) {
        		String script  = (String)this.options.get(OPT_SQL);
        		this.fileSource = new BufferedReader(new StringReader(script));
        	}
        }

        this.cnslSource = new BufferedReader(new InputStreamReader(System.in));

    }

    public void execute()
    {

        while(true) {
            StringBuilder sql = new StringBuilder();

            String line = "";

            do {
                line = readLine("SQL>");
                //System.out.println(line);
                if (line==null || line.length()==0) {
                    continue;
                } else if ("quit".equalsIgnoreCase(line)
                        || "exit".equalsIgnoreCase(line)) {
                    break;
                } else {
                    if(sql.length() > 0) {
                        sql.append(' ');
                    }

                    sql.append(line);

                    if(line.endsWith(";")) {
                        break;
                    }
                }

            } while(true);

            if("quit".equalsIgnoreCase(line)
                    || "exit".equalsIgnoreCase(line)) {
                break;
            } else if(sql.length()>1){
                execInputSQL(sql.toString());
            }
        }
    }

    public void execInputSQL(String sql) {

        Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            if(fileSource!=null)log.info("SQL : {}",sql);

            con = HiveAccessor.getDataSource().getConnection();
            stmt = con.createStatement();

            if (stmt.execute(sql)) {

                int updCount = stmt.getUpdateCount();

                if(updCount>=0) {
                    log.info("{} rows updated!",updCount);
                    return;
                }

                rset = stmt.getResultSet();

                if (fileSource == null) {
                    printPages(rset);
                } else {
                    printAll(rset);
                }
            }

        } catch (SQLException se) {
            log.error(se.getMessage());
            log.info("SQL : {}",sql);
        } catch(TTransportException te) {
        	log.error(te.getMessage());
        	Utils.close(HiveAccessor.getDataSource());

        } finally {
            Utils.close(rset);
            Utils.close(stmt);
            Utils.close(con);
        }
    }

    private void printAll(ResultSet rset) throws SQLException,TTransportException {
        if (rset.next()) {
            ResultSetMetaData meta = rset.getMetaData();
            int colcount = meta.getColumnCount();

            System.out.print("row\t");
            for (int i = 1; i <= colcount; i++) {
                System.out.print(meta.getColumnName(i));
                System.out.print("\t");
            }
            System.out.println();

            int line = 1;
            do {
                System.out.printf("%1$5d\t", line++);
                for (int i = 1; i <= colcount; i++) {
                    System.out.print(format(meta.getColumnType(i),rset.getObject(i)));
                    System.out.print("\t");
                }
                System.out.println();

            } while (rset.next());
        }
    }

    private static final int PAGE_SIZE = 20;
    private void printPages(ResultSet rset) throws SQLException {

        Page page = nextPage(rset);
        while(true) {
            page.printPage();

            if(page.size()<PAGE_SIZE)
            {
            	break;
            }

            page = nextPage(rset);
            if(page.size()==0) {
            	break;
            }

            String input = readLine("[q]uit >");
            if (!"q".equalsIgnoreCase(input)
                    && !"quit".equalsIgnoreCase(input)) {
                continue;
            } else {
                break;
            }
        }
    }

    private static class Page{
        public void printPage() {
        	if(data==null || data.size()==0)
        	{
        		return;
        	}

            StringBuilder sb = new StringBuilder();
            for(int i=0;i<colcount;i++) {
                int cs = colsize[i];
                sb.append("%").append(i+1).append("$")
                .append(getAlign(coltype[i]))
                .append(cs).append("s ");
            }

            sb.append(System.lineSeparator());
            String fmt = sb.toString();
            //log.info(fmt);
            String headerString  = String.format(fmt, (Object[])header);
            System.out.print(headerString);
            char[] line = new char[headerString.length()];
            Arrays.fill(line, '-');
            System.out.println(String.valueOf(line));

            for(Object[] r:data) {
                System.out.printf(fmt,r);
            }
        }
        public int size()
        {
        	return data.size();
        }

		List<String[]> data;
    	String[] header;
    	int[] colsize;
    	int[] coltype;
    	int colcount;
    	public Page(String[] header,int[] colsize, int[] coltype, List<String[]>data)
    	{
    		this.header = header;
    		this.colsize = colsize;
    		this.coltype = coltype;
    		this.data = data;
    		this.colcount = header.length;
    	}
		@Override
		public String toString() {
			return "Page [data.size=" + data.size() + ", header=" + Arrays.toString(header)  + ", colsize="
					+ Arrays.toString(colsize)+ ", coltype="+ Arrays.toString(coltype) + ", colcount=" + colcount + "]";
		}


    }

    private Page nextPage(ResultSet rset)
            throws SQLException {

        ResultSetMetaData meta = rset.getMetaData();

        int colcount = meta.getColumnCount();

        int[] coltype = new int[colcount+1];
        coltype[0]=Types.NUMERIC;
        for(int i=1;i<=colcount;i++)
        {
        	coltype[i] = meta.getColumnType(i);
        }

        String[] header = new String[colcount+1];

        Arrays.fill(header, "");
        header[0]="";
        for (int i = 1; i <= colcount; i++) {
            header[i] = meta.getColumnName(i);
        }


        int[] colsize = new int[colcount+1];

        Arrays.fill(colsize, 1);
        for (int i = 1; i <= colcount; i++) {
            colsize[i] = header[i].length();
        }

        List<String[]> pagedata = new ArrayList<String[]>(PAGE_SIZE);
        while (rset.next()) {
            String[] row = new String[colcount+1];
            pagedata.add(row);

            //Calculate column widths
            int linenumber = rset.getRow();
            row[0] = String.valueOf(linenumber);
            colsize[0] = Math.max(colsize[0], row[0].length());
            for (int i = 1; i <= colcount; i++) {
                row[i] = format(meta.getColumnType(i), rset.getObject(i));
                colsize[i] = Math.max(colsize[i], row[i].length());
            }

            if ((linenumber) % PAGE_SIZE == 0) {
            	break;
            }
        }

        Page page = new Page(header,colsize,coltype,pagedata);
        //log.info(page.toString());
        return page;
    }

    private static String getAlign(int type)
    {
    	String result = "";
        switch(type) {
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.BIGINT:
            case Types.BIT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                result="";
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            	result="-";
            	break;
            case Types.VARCHAR:
            	result="-";
            	break;
            default:
            	result="-";
        }
        return result;

    }
    private String format(int type, Object data)
    {
        if(data == null)return "";

        String result = data.toString();
        switch(type) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                result = formatDecimal(data);
                break;
            case Types.BIGINT:
            case Types.BIT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                String.format("%,d", data);
                break;
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                result = String.format("%.3f", data);
                result = result.replaceAll("\\.0+$", "");
                break;
            case Types.DATE:
                result = String.format("%1$tF", data);
                break;
            case Types.TIME:
                result = String.format("%1$tT", data);
                break;
            case Types.TIMESTAMP:
                result = String.format("%1$tF %1$tT %1$tL", data);
                break;
        }
        return result;

    }

    private String formatDecimal(Object data)
    {
        if(data==null)return "";
        DecimalFormat formatter = new DecimalFormat("0.000");
        formatter.setMaximumFractionDigits(3);
        formatter.setMinimumFractionDigits(1);
        String formatted = formatter.format(data);
        formatted = formatted.replaceAll("\\.0+$", "");
        return formatted;
    }

    private String readLine(String prompt) {
        String str = null;

        if (this.fileSource != null) {
            try {
                str = this.fileSource.readLine();

                this.fileSource.mark(1);
                int nextc = this.fileSource.read();
                if(str!=null && nextc==-1) {
                	str = str.trim();
                	if(!str.endsWith(";")) {
                		str=str+";";
                	}
                }
                this.fileSource.reset();

                if(str==null) {
                	str="exit";
                }


            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
        } else {
            try {
                System.out.print(System.lineSeparator()+prompt);
                str = this.cnslSource.readLine();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        if (str == null)
            return "";
        else
            return str.trim();
    }

}