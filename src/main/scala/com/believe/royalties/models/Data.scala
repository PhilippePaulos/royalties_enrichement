package com.believe.royalties.models

case class Album(upc: String, album_name: String, label_name: String, country: String)
case class Sale(PRODUCT_UPC: String, TRACK_ISRC_CODE: String, TRACK_ID: BigInt, DELIVERY: String, NET_TOTAL: Double,
                TERRITORY: String)
case class Song(isrc: String, song_id: BigInt, song_name: String, artist_name: String, content_type: String)
