package solr.jdbc.extension.s2jdbc.entity;

import java.sql.Date;
import java.sql.Timestamp;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Player {
	@Id
	public Integer playerId;
	public String playerName;
	public String team;
	public Date registeredAt;
}
