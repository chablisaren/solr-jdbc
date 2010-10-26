package solr.jdbc.extension.s2jdbc;

import java.util.List;

import javax.transaction.UserTransaction;

import org.seasar.extension.jdbc.JdbcManager;
import org.seasar.extension.unit.S2TestCase;

import solr.jdbc.extension.s2jdbc.entity.Player;
import solr.jdbc.extension.s2jdbc.service.PlayerService;

public class S2JDBCTest extends S2TestCase {
	PlayerService playerService;
	JdbcManager jdbcManager;
	UserTransaction userTransaction;

	@Override
	public void setUp() {
		include("app.dicon");
		jdbcManager.updateBySql("CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), registered_at DATE)").execute();
	}

	public void test_Entity() throws Exception {
		Player player1 = new Player();
		player1.playerId = 1;
		player1.playerName = "高橋慶彦";
		player1.team = "カープ";
		userTransaction.begin();
		playerService.insert(player1);
		userTransaction.commit();

		List<Player> playerList = playerService.findAll();
	}
}
