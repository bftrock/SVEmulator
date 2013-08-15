using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Npgsql;

namespace SVEmulator
{
    public partial class Form1 : Form
    {

        private NpgsqlConnection conn = new NpgsqlConnection();
        private NpgsqlCommand cmd = new NpgsqlCommand();
        const string HOST = "localhost";
        const string PORT = "5432";
        const string UID = "sv_user";
        const string PWD = "$m@rtView";
        const string DBNAME = "smartview";
        const int TREND_MAX_IDX = 600;
        const int FRAME_MAX_IDX = 1000;
        const int FRAME_MINUTES = 1;
        private int trendIndex = 0;
        private int frameIndex = 0;
        private int lastIntValue = 0;

        public Form1()
        {
            InitializeComponent();
        }

        private void StartButton_Click(object sender, EventArgs e)
        {
            string sql, sqlVal;
            int i, j;
            double omega, theta;
            DateTime ts = DateTime.Now;

            #region ConnectToDatabase
            try
            {
                conn.ConnectionString = String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};",
                    HOST, PORT, UID, PWD, DBNAME);
                conn.Open();
                LogMessage("Connection successfully opened");
            }
            catch (Exception msg)
            {
                LogMessage("Error: " + msg.ToString());
                return;
            }
            #endregion            

            #region InitializeRealTimeTable
            cmd.Connection = conn;
            sql = "DROP TABLE IF EXISTS rt";
            ExecuteCommand(sql);
            LogMessage("RT table successfully deleted");
            sql = "CREATE TABLE rt (ts timestamp,";
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("n{0} double precision,", i);
            }
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("i{0} integer,", i);
            }
            sql = sql.Substring(0, sql.Length - 1) + ")";
            ExecuteCommand(sql);
            LogMessage("RT table successfully created");
            sql = String.Format("INSERT INTO rt (ts) VALUES ('{0}')", ts.ToString("yyyy-MM-dd HH:mm:ss"));
            ExecuteCommand(sql);
            #endregion

            #region InitializeTrendTable
            sql = "DROP TABLE IF EXISTS trend";
            ExecuteCommand(sql);
            LogMessage("Trend table successfully deleted");
            sql = "CREATE TABLE trend (idx INTEGER PRIMARY KEY, ts TIMESTAMP,";
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("n{0} double precision DEFAULT 0,", i);
            }
            sql = sql.Substring(0, sql.Length - 1) + ")";
            ExecuteCommand(sql);
            LogMessage("Trend table successfully created");
            sql = "INSERT INTO trend (idx) VALUES ";
            for (j = 0; j < TREND_MAX_IDX; j++)
            {
                sqlVal = String.Format("('{0}')", j);
                ExecuteCommand(sql + sqlVal);
            }
            LogMessage("Trend table successfully populated");
            #endregion

            #region InitializeFrameTable
            sql = "DROP TABLE IF EXISTS frame";
            ExecuteCommand(sql);
            LogMessage("Frame successfully deleted");
            sql = "CREATE TABLE frame (idx INTEGER PRIMARY KEY, ts TIMESTAMP,";
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("n{0} double precision DEFAULT 0,", i);
            }
            sql = sql.Substring(0, sql.Length - 1) + ")";
            ExecuteCommand(sql);
            LogMessage("Frame successfully created");

            // Populate frame table with data
            sql = "INSERT INTO frame (idx) VALUES ";
            for (j = 0; j < FRAME_MAX_IDX; j++)
            {
                sqlVal = String.Format("('{0}')", j);
                ExecuteCommand(sql + sqlVal);
            }
            LogMessage("Frame table successfully populated");
            #endregion

            // Start timer
            PollTimer.Interval = 1000;
            PollTimer.Enabled = true;
            LogMessage("Timer enabled");

            // Change button states to reflect successful start-up
            StartButton.Enabled = false;
            StopButton.Enabled = true;
            LogMessage("SmartView started");
        }

        private void StopButton_Click(object sender, EventArgs e)
        {
            // Stop timer since we're shutting down
            PollTimer.Enabled = false;
            LogMessage("Timer disabled");

            // Close connection to database
            conn.Close();
            LogMessage("Connection closed");

            // Change button states to be ready to start again
            StartButton.Enabled = true;
            StopButton.Enabled = false;
            LogMessage("SmartView stopped");
        }

        private void PollTimer_Tick(object sender, EventArgs e)
        {
            string sql;
            int i;
            DateTime ts = DateTime.Now;
            string tsf = ts.ToString("yyyy-MM-dd HH:mm:ss");
            double[] sampledValues = new double[10];
            int[] intValues = new int[10];
            double omega, theta;

            #region GetSampledValues
            for (i = 0; i < 10; i++)
            {
                omega = trendIndex * Math.PI / 30;
                for (i = 0; i < 10; i++)
                {
                    // Phase shifted a little for each column
                    theta = i * Math.PI / 10;
                    sampledValues[i] = Math.Sin(omega + theta);
                }
            }
            lastIntValue++;
            for (i = 0; i < 10; i++)
            {
                intValues[i] = lastIntValue + i;
            }
            #endregion

            #region UpdateRealTimeTable
            sql = String.Format("UPDATE rt SET ts='{0}',", tsf);
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("n{0}='{1}',", i, sampledValues[i]);
            }
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("i{0}='{1}',", i, intValues[i]);
            }
            sql = sql.Substring(0, sql.Length - 1);
            ExecuteCommand(sql);
            #endregion

            #region UpdateTrendTable
            sql = String.Format("UPDATE trend SET ts='{0}',", tsf);
            for (i = 0; i < 10; i++)
            {
                sql += String.Format("n{0}='{1}',", i, sampledValues[i]);
            }
            sql = sql.Substring(0, sql.Length - 1) + String.Format(" WHERE idx='{0}'", trendIndex);
            ExecuteCommand(sql);
            #endregion

            #region UpdateFrameTable
            if ((ts.Minute % FRAME_MINUTES == 0) && (ts.Second == 0))
            {
                string startTime = ts.AddMinutes(-FRAME_MINUTES).ToString("yyyy-MM-dd HH:mm:ss");
                System.Data.Common.DbDataReader dbr;

                sql = "SELECT ";
                for (i = 0; i < 10; i++)
                {
                    sql += String.Format("AVG(n{0}),", i);
                }
                sql = String.Format("{0} FROM trend WHERE ts > '{1}'", sql.Substring(0, sql.Length - 1), startTime);
                dbr = ExecuteQuery(sql);
                if (dbr.HasRows)
                {
                    sql = String.Format("UPDATE frame SET ts='{0}',", tsf);
                    dbr.Read();
                    for (i = 0; i < 10; i++)
                    {
                        sql += String.Format("n{0}='{1}',", i, dbr.GetValue(i));
                    }
                    dbr.Close();
                    sql = sql.Substring(0, sql.Length - 1) + String.Format(" WHERE idx='{0}'", frameIndex);
                    ExecuteCommand(sql);
                    LogMessage("Frame table updated");
                }
                frameIndex++;
                if (frameIndex == FRAME_MAX_IDX)
                    frameIndex = 0;
            }
            #endregion

            trendIndex++;
            if (trendIndex == TREND_MAX_IDX)
                trendIndex = 0;
        }

        private void LogMessage(string msg, bool isQuery = false)
        {
            DateTime now = DateTime.Now;
            string[] msgArr = new string[] {now.ToString(), msg};
            ListViewItem newItem = new ListViewItem(msgArr);
            lvLog.Items.Add(newItem);
        }

        private int ExecuteCommand(string sql)
        {
            int numRowsAffected;

            try
            {
                cmd.CommandText = sql;
                numRowsAffected = cmd.ExecuteNonQuery();
                return numRowsAffected;
            }
            catch (Exception ex)
            {
                LogMessage("Could not execute command: " + ex.ToString());
                return 0;
            }
        }
        
        private System.Data.Common.DbDataReader ExecuteQuery(string sql)
        {
            try
            {
                cmd.CommandText = sql;
                System.Data.Common.DbDataReader dbr;
                dbr = cmd.ExecuteReader(CommandBehavior.SingleResult);
                return dbr;
            }
            catch (Exception ex)
            {
                LogMessage("Could not execute query: " + ex.ToString());
                return null;
            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

    }
}
