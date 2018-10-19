#David T
#13515131

import os
from flask import Flask, render_template, request, redirect
from cassandra.cluster import Cluster
import time
import uuid

app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html')

@app.route("/newuser", methods=['GET'])
def new_user_get():
    return render_template('newuser.html')

@app.route("/newuser", methods=['POST'])
def new_user_post():
    cluster = Cluster(['167.205.35.20'])
    session = cluster.connect("davidtsaksomo")
    uname = request.form.get('uname')
    password = request.form.get('password')
    user_insert_stmt = session.prepare("INSERT INTO users (username, password) VALUES (?, ?)")
    session.execute(user_insert_stmt, (uname, password))
    cluster.shutdown()
    return render_template('newuser.html', status = 'Berhasil')

@app.route("/follow", methods=['GET'])
def follow_get():
    return render_template('follow.html')

@app.route("/follow", methods=['POST'])
def follow_post():
    cluster = Cluster(['167.205.35.20'])
    session = cluster.connect("davidtsaksomo")
    uname = request.form.get('uname')
    follow_uname = request.form.get('follow_uname')
    user_select_stmt = session.prepare("SELECT count(*) FROM users WHERE username=?")
    user = session.execute(user_select_stmt, (uname,))
    follow_user = session.execute(user_select_stmt, (follow_uname,))
    if (user[0].count == 0):
        return render_template('follow.html', status = 'username tidak ditemukan')
    if (follow_user[0].count == 0):
        return render_template('follow.html', status = 'username yang ingin difollow tidak ditemukan')
    followers_insert_stmt = session.prepare("INSERT INTO followers (username, follower, since) VALUES (?, ?, ?)")
    friends_insert_stmt = session.prepare("INSERT INTO friends (username, friend, since) VALUES (?, ?, ?)")
    session.execute(followers_insert_stmt, (follow_uname, uname, time.time()))
    session.execute(friends_insert_stmt, (uname, follow_uname, time.time()))
    cluster.shutdown()
    return render_template('follow.html', status = 'Berhasil')

@app.route("/tweet", methods=['GET'])
def tweet_get():
    return render_template('tweet.html')

@app.route("/tweet", methods=['POST'])
def tweet_post():
    cluster = Cluster(['167.205.35.20'])
    session = cluster.connect("davidtsaksomo")
    uname = request.form.get('uname')
    tweet = request.form.get('tweet')
    user_select_stmt = session.prepare("SELECT count(*) FROM users WHERE username=?")
    user = session.execute(user_select_stmt, (uname,))
    if (user[0].count == 0):
        return render_template('tweet.html', status = 'username tidak ditemukan')
    tweet_id = uuid.uuid4()
    tweet_insert_stmt = session.prepare("INSERT INTO tweets (tweet_id, username, body) VALUES (?, ?, ?)")
    userline_insert_stmt = session.prepare("INSERT INTO userline (username, time, tweet_id) VALUES (?, now(), ?)")
    timeline_insert_stmt = session.prepare("INSERT INTO timeline (username, time, tweet_id) VALUES (?, now(), ?)")
    followers_select_stmt = session.prepare("SELECT * FROM followers WHERE username = ?")
    session.execute(tweet_insert_stmt, (tweet_id, uname, tweet))
    session.execute(userline_insert_stmt, (uname, tweet_id))
    session.execute(timeline_insert_stmt, (uname, tweet_id))
    rows = session.execute(followers_select_stmt, (uname,))
    for row in rows:
        session.execute(timeline_insert_stmt, (row.follower, tweet_id))
    cluster.shutdown()
    return render_template('tweet.html', status = 'Berhasil')

@app.route("/usertweet", methods=['GET'])
def usertweet_get():
    return render_template('usertweet.html')

@app.route("/usertweet", methods=['POST'])
def usertweet_post():
    cluster = Cluster(['167.205.35.20'])
    session = cluster.connect("davidtsaksomo")
    uname = request.form.get('uname')
    user_select_stmt = session.prepare("SELECT count(*) FROM users WHERE username=?")
    user = session.execute(user_select_stmt, (uname,))
    if (user[0].count == 0):
        return render_template('usertweet.html', status = 'username tidak ditemukan')
    userline_select_stmt = session.prepare("SELECT tweet_id, dateOf(time) FROM userline WHERE username = ?")
    tweet_select_stmt = session.prepare("SELECT body FROM tweets WHERE tweet_id = ?")
    rows = session.execute(userline_select_stmt, (uname,))
    a = ""
    for row in rows:
        body = session.execute(tweet_select_stmt, (row[0],))
        a += str(row[1]) + "<br/>" + uname + ": " + body[0][0] + "<br/><br/>"
    cluster.shutdown()
    return render_template('usertweet.html', status = a)

@app.route("/timeline", methods=['GET'])
def timeline_get():
    return render_template('timeline.html')

@app.route("/timeline", methods=['POST'])
def timeline_post():
    cluster = Cluster(['167.205.35.20'])
    session = cluster.connect("davidtsaksomo")
    uname = request.form.get('uname')
    user_select_stmt = session.prepare("SELECT count(*) FROM users WHERE username=?")
    user = session.execute(user_select_stmt, (uname,))
    if (user[0].count == 0):
        return render_template('timeline.html', status = 'username tidak ditemukan')
    timeline_select_stmt = session.prepare("SELECT tweet_id, dateOf(time) FROM timeline WHERE username = ?")
    tweet_select_stmt = session.prepare("SELECT body, username FROM tweets WHERE tweet_id = ?")
    rows = session.execute(timeline_select_stmt, (uname,))
    a = ""
    for row in rows:
        body = session.execute(tweet_select_stmt, (row[0],))
        a += str(row[1]) + "<br/>" + body[0][1] + ": " + body[0][0] + "<br/><br/>"
    cluster.shutdown()
    return render_template('timeline.html', status = a)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8111)

