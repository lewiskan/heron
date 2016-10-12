// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.kx;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


  /**
   * A bolt that opens kdb-plus connection and writes field
   * http://code.kx.com/wiki/Cookbook/InterfacingWithJava
   *
   * Opens connection conn (localhost, port 5001)
   * execute method calls user logic on conn
   *
   * Launch localhost kdb+ listening on port 5001
   * $ q -p 5001
   */

  public final class KdbplusSinkBolt extends BaseRichBolt {
    private OutputCollector collector;
    private c conn;

    public c conn(){
        try {
            c conn = new c("localhost", 5001);
            return conn;
            } catch (Exception ex) {
                System.out.println("Conn exception - replace w/ Logger");
                return null;
           } 
        }

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
      conn = conn();
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      if (key == null) {
        throw new IllegalArgumentException("tuple.getString(0) null " + tuple.toString());
      } else {
        System.out.println("key: "+key);
        if (conn != null) {
            /* User kdb+ query
            * String query="([]date:.z.D;time:.z.T;sym:n?`8;price:`float$n?500.0;size:n?100;r:til (n:100))";
            * c.k(query);
            */
            try{conn.close();
            } catch (IOException ex) {}
        }
      }
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }