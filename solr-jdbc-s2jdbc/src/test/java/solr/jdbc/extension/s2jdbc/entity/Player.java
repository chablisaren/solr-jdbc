package solr.jdbc.extension.s2jdbc.entity;

import java.sql.Date;
import java.sql.Timestamp;

import javax.persistence.Entity;

@Entity
public class Player {
	public Integer playerId;
	public String playerName;
	public String team;
	public Date registeredAt;
}
