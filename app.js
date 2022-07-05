const express = require('express');
const app = express();
const Pool=require('pg').Pool

app.use(express.static('public'));
app.use(express.urlencoded({extended: false}));
app.listen(3000);
//connet to database
const pool= new Pool({
    user: 'neo',
    host: 'localhost',
    database: 'lab4',
    password: 'thematrix',
    port: 5432,
});

//root page
app.get('/',(req,res)=>{
    pool.query('select distinct season_year from match order by season_year',(err,r)=>{
        res.render("first.ejs",{title:"home",year:r.rows});
    });
});
// URL:/matches
app.get("/matches",(req,res)=>{
    pool.query(
        `select match_id,t1.team_name as team1,t2.team_name as team2,venue_name,t3.team_name as winner,city_name 
        from match join venue 
        on match.venue_id=venue.venue_id  
        join team t1 on match.team1=t1.team_id  
        join team t2 on match.team2=t2.team_id
        join team t3 on match.match_winner=t3.team_id
        order by season_year,match_id
        `,
        (error,results)=>{
            if(error)
                throw error;
            //res.send(results);

            res.render("matches.ejs",{matches:results.rows,title:"match"});
        }
    );
});

//match-info
app.get("/matches/:id", async (req,res)=>{
    //batsman table:-player id and name,runs,fours,sixes,balls
    let temp=[`select bb.innings_no,player.player_id as id, player.player_name as name, sum(runs_scored) as runs , sum(case when (runs_scored=4) then 1 else 0 end) as fours, sum(case when (runs_scored=6) then 1 else 0 end) as six, count(ball_id) as balls_faced
    from ball_by_ball bb
    join match on match.match_id = bb.match_id
    join player on bb.striker = player.player_id
    where bb.match_id = ${req.params.id}
    group by player.player_id,bb.match_id, bb.innings_no, player.player_name
    order by bb.innings_no, runs desc, player.player_name `,
    //extras
    `select bb.innings_no, sum(extra_runs) as extras
    from ball_by_ball bb
    join match on match.match_id = bb.match_id
    where bb.match_id = ${req.params.id}
    group by bb.innings_no
    order by bb.innings_no`,
    //total and wickets
    `select bb.innings_no, sum(runs_scored + extra_runs) as total_runs, sum(case when (out_type not in('NULL')) then 1 else 0 end) as wickets,sum(case when (ball_id<7) then 1 else 0 end ) as balls
    from ball_by_ball bb
    join match on match.match_id = bb.match_id
    where bb.match_id = ${req.params.id}
    group by  bb.innings_no
    order by  bb.innings_no`,
    //bowlers table:- innings,player id and name,balls,runs,wickets
    `select bb.innings_no,player.player_id as id, player.player_name as name, count(ball_id) as balls_bowled, sum(runs_scored) as runs_given, sum(case when (out_type not in('NULL')) then 1 else 0 end) as wickets
	from ball_by_ball bb
	join match on match.match_id = bb.match_id
	join player on bb.bowler = player.player_id
    where bb.match_id=${req.params.id}
	group by player.player_id, bb.innings_no, player.player_name
	order by  bb.innings_no, wickets desc, player.player_name`,
    //inn:-first,second innings teams and ids
    `select tt1.team_id as id1, tt1.team_name as first,tt2.team_id as id2, tt2.team_name as second  from
    (select team1 as t1,team2 as t2 from match where match.match_id=${req.params.id} and ((toss_winner=team1 and toss_name='bat') or (toss_winner=team2 and toss_name='field'))
    union
    select team2 as t1,team1 as t2 from match where match.match_id=${req.params.id} and ((toss_winner=team2 and toss_name='bat') or (toss_winner=team1 and toss_name='field')))foo
    join team tt1 on foo.t1=tt1.team_id
    join team tt2 on foo.t2=tt2.team_id
    `,
    //umpires
    `select match_id,umpire_name,role_desc 
    from umpire_match join umpire on umpire.umpire_id=umpire_match.umpire_id
    where match_id=${req.params.id}
    `,
    //playing XI
    `select team_id, player.player_name 
    from player_match join player 
    on player_match.player_id=player.player_id 
    where match_id=${req.params.id}`,
    //Rpo:-innings no,runs per over,wickets no in each over
    `select  innings_no,over_id,sum(runs_scored)+sum(extra_runs) as runs,
    sum(case when out_type in ('caught', 'caught and bowled' , 'bowled' , 'stumped' , 'keeper catch', 'lbw', 'run out', 'hit wicket','retired hurt') then 1 else 0 end )
    as wicket
    from ball_by_ball where match_id=${req.params.id}
    group by innings_no,over_id
    order by innings_no,over_id`,

    //match-info 1,2 team name and id,match_id,venue,year,winner,toss,win,margin
    `select win_type,win_margin, tt1.team_id as id1, tt1.team_name as team1,tt2.team_id as id2, tt2.team_name as team2,tt3.team_name as toss, match_id, season_year as year, venue_name as venue, tt4.team_name as winner ,venue.venue_id
    from match
    join team tt1 on match.team1 = tt1.team_id
    join team tt2 on match.team2 = tt2.team_id
    join team tt3 on match.toss_winner=tt3.team_id
    join team tt4 on match.match_winner=tt4.team_id
    join venue on match.venue_id=venue.venue_id
    where match.match_id =${req.params.id}`,
    //runs-percentage
    `select 
    sum(case when (runs_scored=1) then 1 else 0 end) as one,
    sum(case when (runs_scored=2) then 2 else 0 end) as two,
    sum(case when (runs_scored=3) then 3 else 0 end) as three,
    sum(case when (runs_scored=4) then 4 else 0 end) as four,
    sum(case when (runs_scored=6) then 6 else 0 end) as six,
    sum(extra_runs) as extra
        from ball_by_ball
        where match_id =${req.params.id} 
        group by innings_no`
    ]
   
    try{
        
        const bat=await pool.query(temp[0]);
        const extra=await pool.query(temp[1]);
        const total=await pool.query(temp[2]);
        const bowler=await pool.query(temp[3]);
        const innings=await pool.query(temp[4]);
        const umpires=await pool.query(temp[5]);
        const players=await pool.query(temp[6]);
        const dat=await pool.query(temp[7]);
        const info=await pool.query(temp[8]);
        const runs= await pool.query(temp[9]);
        res.render("match-info.ejs",{bat:bat.rows,extra:extra.rows,total:total.rows,
            bowler:bowler.rows,inn:innings.rows,umpires:umpires.rows,players:players.rows,
            Rpo:dat.rows,info:info.rows,run:runs.rows,
             title:"match-info"});
    }
    catch(err){console.error(err.message);}

});


app.get("/players/:id", async (req,res)=>{
    let temp=[
        `select player_name as name,country_name as country,batting_hand as hand,bowling_skill as skill
        from player where player_id=${req.params.id}`,
        //outs,runs,balls,four,six
        `select sum(case when out_type!='NULL' then 1 else 0 end)as outs, count(*) as balls, sum(runs_scored) as runs,sum(case when runs_scored=4 then 1 else 0 end) as four ,
        sum(case when runs_scored=6 then 1 else 0 end) as six from ball_by_ball where striker=${req.params.id}`,
        //max-runs
        `select max(sum) from (select sum(runs_scored)
        from ball_by_ball
        where ball_by_ball.striker=${req.params.id} 
        group by match_id) as run `,
        //50s
        `select match_id,sum(runs_scored)
        from ball_by_ball
        where ball_by_ball.striker=${req.params.id} 
        group by match_id
        having sum(runs_scored) >= 50 and sum(runs_scored) <100`,
        //match vs runs
        `select match_id, sum(case when striker=${req.params.id} then runs_scored
            when non_striker=${req.params.id} then 0 end) as runs
         from ball_by_ball
         where ball_by_ball.striker=${req.params.id} or non_striker=${req.params.id}
         group by match_id order by match_id`,

         //BOWLER SECTION
         //bStat balls,runs,wickets
        `select sum(runs_scored) as runs,count(*) as balls,
        sum(case when out_type not in ('NULL','retired hurt','run out') then 1 else 0 end) as wickets 
        from ball_by_ball where bowler=${req.params.id}
        `,
        //five.Take its length as no of matches
        `select count(out_type),match_id
        from ball_by_ball
        where bowler=${req.params.id} and out_type!='NULL'
        group by match_id
        having count(out_type) >= 5;`,
        //mrw match,runs,wickets for graph
        `select match_id,sum(runs_scored) as runs,
        sum(case when out_type not in ('NULL','retired hurt','run out') then 1 else 0 end) as wickets 
        from ball_by_ball where bowler=${req.params.id}
        group by match_id
        order by match_id`

    ]
    try{
        const profile=await pool.query(temp[0]);
        const stat=await pool.query(temp[1]);
        const maxR=await pool.query(temp[2]);
        const fifty=await pool.query(temp[3]);
        const mvr= await pool.query(temp[4]);
        const bstat= await pool.query(temp[5]);
        const five=await pool.query(temp[6]);
        const mrw=await pool.query(temp[7]);
        res.render("profile.ejs",{profile:profile.rows,stat:stat.rows,maxR:maxR.rows,mvr:mvr.rows,fifty:fifty.rowCount,
            bstat:bstat.rows ,five:(five.rows).length,mrw:mrw.rows, title:"player name"})
    }
    catch(e){console.error(e.message)};
});
app.get("/venues",(req,res)=> {
    pool.query(
    `select venue_name, venue_id from venue order by venue_name`,
    (error,results)=> {
        if(error)
            throw error;
        res.render("venues.ejs", {venues:results.rows, title:"venues"});
    }
    );

});

app.get("/players",(req,res)=>{
    temp=[`select player_name as name,player_id as id from player order by player_name`]
    pool.query(temp[0],(e,r)=>{

        res.render("players.ejs",{title:"players",list:r.rows})
    })
})

app.get("/venue/:id", async (req,res)=>{
    let temp =[`select venue_name, capacity, city_name from venue where venue.venue_id = ${req.params.id}`, 
                `select count(match_id) as total_match from match where match.venue_id = ${req.params.id}`,
                `select max (highest_runs) as highest_runs from (select sum(runs_scored) + sum(extra_runs) as highest_runs from ball_by_ball inner join match on ball_by_ball.match_id = match.match_id where match.venue_id = ${req.params.id} group by match.match_id) as temp;`,
                `select min (lowest_runs) as lowest_runs from (select sum(runs_scored) + sum(extra_runs) as lowest_runs from ball_by_ball inner join match on ball_by_ball.match_id = match.match_id where match.venue_id = ${req.params.id} group by match.match_id) as temp;`,
                `with win as(
                    (select team1 as t1,team2 as t2,match_winner  
                     from match where match.venue_id=${req.params.id} and 
                     ((toss_winner=team1 and toss_name='bat') or (toss_winner=team2 and toss_name='field'))
                        union
                        select team2 as t1,team1 as t2,match_winner 
                     from match where match.venue_id=${req.params.id} and 
                     ((toss_winner=team2 and toss_name='bat') or (toss_winner=team1 and toss_name='field')))
                    )
                    select sum(case when t1=match_winner then 1 else 0 end) as first,
                    sum(case when t2=match_winner then 1 else 0 end) as second from win`,
                `with runs as(
                    select match_id as iid,sum(runs_scored)+sum(extra_runs) as runs 
                    from ball_by_ball 
                    where innings_no=1 group by match_id)
                    select season_year,sum(runs),count(iid) from runs join match on match.match_id=runs.iid
                    where venue_id=${req.params.id}
                    group by season_year`
                ]
    const bla = await pool.query(temp[0]);
    const bla1 = await pool.query(temp[1]);
    const bla2 = await pool.query(temp[2]);
    const bla3 = await pool.query(temp[3]);
    const pie=await pool.query(temp[4]);
    const avg=await pool.query(temp[5]);
    res.render("venue-info.ejs",{bla : bla.rows,bla1:bla1.rows,bla2:bla2.rows,bla3:bla3.rows,
        pie:pie.rows,avg:avg.rows,title:"venue-info"} )
})

app.get("/pointstable/:id",(req,res)=>{
    temp=[
    `select team_name,Mat,Won,Mat-Won as Lost,0 as Tied, 
    ROUND(coalesce(sum(case when team_1=team_id then runs_1 else runs_2 end),0)*1.0/coalesce(sum(case when team_id=team_1 then max_1 else max_2 end),1)-coalesce(sum(case when team_2=team_id then runs_1 else runs_2 end),0)*1.0/coalesce(sum(case when team_id=team_2 then max_1 else max_2 end),1),3) as NR,
    2*Won as pts from (select team_name,team_id,coalesce(count(match_id),0) as Mat,
    coalesce(count(match_id) filter(where match_winner=team_id),0) as Won 
    from team 
    left outer join 
    (select * from match where season_year =${req.params.id}) as match1 on team_id=team1 or team_id=team2 group by (team_id)) as w 
    left outer join (select b.match_id,case when win_type='runs' then match_winner else team1+team2-match_winner end as team_1,coalesce(sum(runs_scored+extra_runs) filter(where innings_no=1),0) as runs_1,
    coalesce(max(over_id) filter(where innings_no=1),0) as max_1,
    case when win_type='wickets' then match_winner else team1+team2-match_winner end as team_2,
    coalesce(sum(runs_scored+extra_runs) filter(where innings_no=2),0) as runs_2,
    coalesce(max(over_id) filter(where innings_no=2),0) as max_2 from match as m 
    inner join ball_by_ball as b on b.match_id=m.match_id where season_year =${req.params.id} 
    group by (b.match_id,win_type,team1,team2,match_winner)) as r 
    on team_id=team_1 or team_id=team_2 where Mat<>0 
    group by (team_name,Mat,Won) order by pts desc;
    `]
    pool.query(temp[0],(e,r)=>{
        if(e) console.error(e.message)
        res.render("pointstable.ejs",{table:r.rows,title:req.params.id})
    })
})

app.post("/createVenue",(req,res)=>{
    console.log(req.body)
    pool.query(
        'insert into venue set ? ;',
        [req.body.VenueName,req.body.CountryName,req.body.CityName,req.body.Capacity],
        (err,r)=>{
            console.log("Venue added successfully ");
            res.redirect("/venues")
        }
    )
})

app.get("/venues/add",(req,res)=>{
    res.render("addVenue.ejs",{title:"add venue"})
})