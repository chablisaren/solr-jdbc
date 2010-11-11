package com.google.code.solr_jdbc.extension.s2jdbc.entity;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Player {
	@Id
	public Integer playerId;
	public String playerName;
	public String team;
	public List<String> position; 
	public Date registeredAt;
}
