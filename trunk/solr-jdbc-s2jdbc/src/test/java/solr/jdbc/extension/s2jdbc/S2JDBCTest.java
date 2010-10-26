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
	}

	
	public void testSearchSimple() throws Exception {
		jdbcManager.updateBySql("DROP TABLE PLAYER").execute();
		jdbcManager.updateBySql("CREATE TABLE PLAYER(PLAYER_ID number, TEAM varchar(10), PLAYER_NAME varchar(50), REGISTERED_AT DATE)").execute();
		Player player1 = new Player();
		player1.playerId = 1;
		player1.playerName = "高橋慶彦";
		player1.team = "カープ";
		userTransaction.begin();
		playerService.insert(player1);
		userTransaction.commit();

		List<Player> playerList = playerService.findAll();
		assertEquals(1, playerList.size());
		Player player = playerList.get(0);
		assertEquals("Entityに値が入っている", "高橋慶彦", player.playerName);
		assertNull("設定しなかったところはnullが入る", player.registeredAt);
	}
}
