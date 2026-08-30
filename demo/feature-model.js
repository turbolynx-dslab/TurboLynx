/* Small, explicit teaching models. Never used as engine measurements. */
(function(root){
  'use strict';
  const schemas=[
    {label:'Music · basic',schema:['name','kind','genre','follows'],kind:'person',family:'music'},
    {label:'Music · +verified',schema:['name','kind','genre','follows','verified'],kind:'person',family:'music'},
    {label:'Old Town · basic',schema:['name','kind','neighborhood','since'],kind:'person',family:'oldtown'},
    {label:'Old Town · +verified',schema:['name','kind','neighborhood','since','verified'],kind:'person',family:'oldtown'},
    ...['title','noise','junk0','junk1','bot0','bot1','bot2','bot3','bot4','bot5'].map(key=>({label:key,schema:[key],kind:null,family:'other'}))
  ];
  const profileSections=[
    ['Basics','venue_type capacity rating price_level age_limit'],
    ['Hours','opens_at closes_at closed_days last_entry timezone'],
    ['Amenities','wifi coat_check lockers charging_points air_conditioning'],
    ['Accessibility','wheelchair_access step_free_entry accessible_restroom hearing_support service_animals'],
    ['Music','primary_genre live_music_days sound_system dance_floor noise_level'],
    ['Food & drink','serves_food cuisine signature_drink vegetarian_options food_last_order'],
    ['Reservations','reservation_required walk_ins booking_url cancellation_window group_booking'],
    ['Pricing','average_spend cover_charge minimum_spend currency service_charge'],
    ['Transit','nearest_station transit_lines walking_minutes taxi_stand bike_parking'],
    ['Parking','parking_available parking_spaces valet_parking parking_fee ev_charging'],
    ['Events','event_type event_frequency ticketing average_duration audience_size'],
    ['Atmosphere','ambience dress_code lighting crowd_age conversation_level'],
    ['Outdoor','outdoor_seating rooftop smoking_area weather_cover patio_heaters'],
    ['Family','family_friendly minimum_age stroller_access kids_menu changing_table'],
    ['Safety','security_staff bag_check cctv emergency_exits first_aid'],
    ['Payments','accepts_cash accepts_cards contactless split_bills payment_service_charge'],
    ['Contact','phone email website social_handle booking_contact'],
    ['Location','district street postal_code latitude longitude'],
    ['Sustainability','recycling renewable_energy reusable_cups local_sourcing sustainability_cert'],
    ['Pets','pet_friendly water_bowls pet_area pet_events leash_required'],
    ['Stage','stage_width stage_depth backstage_rooms green_room loading_access'],
    ['Acoustics','acoustic_treatment decibel_limit monitor_system microphone_count mixing_console'],
    ['Seating','seating_style table_count bar_seats booth_count flexible_layout'],
    ['Restrooms','restroom_count unisex_restroom accessible_stall baby_changing restroom_attendant'],
    ['Connectivity','public_wifi wifi_speed cellular_reception livestream_support av_network'],
    ['Hospitality','table_service counter_service host_desk queue_system cloakroom'],
    ['Policies','reentry_allowed photography outside_food refund_policy lost_and_found'],
    ['Check-in','checkin_method qr_entry id_required average_wait capacity_updates'],
    ['Promotions','happy_hour member_discount student_discount loyalty_program promo_channel'],
    ['Neighborhood','nearby_landmark safety_at_night foot_traffic neighborhood_style nearby_hotels'],
    ['Weather','indoor_backup rain_policy seasonal_hours climate_control umbrella_storage'],
    ['Art','art_style rotating_exhibits local_artists gallery_space installation_count'],
    ['Community','community_events nonprofit_partner volunteer_program local_hiring community_score'],
    ['Corporate','corporate_events meeting_rooms projector catering invoice_payment'],
    ['Production','house_lights projection recording technician_available power_capacity'],
    ['Inventory','chair_inventory table_inventory glassware_capacity storage_rooms kitchen_capacity'],
    ['Languages','service_languages multilingual_signage translation_support menu_languages staff_languages'],
    ['Compliance','liquor_license occupancy_permit fire_inspection food_grade inspection_date'],
    ['Technology','mobile_ordering digital_menu guest_app beacon_support analytics_opt_out'],
    ['History','established_year former_name founder renovated_year heritage_status']
  ].map(([name,fields])=>({name,fields:fields.split(' ')}));
  const profileExamples=[
    'live music club|450|4.6|3|21','18:00|02:00|Mon|01:15|America/Los_Angeles',
    'yes|yes|no|12|yes','yes|yes|yes|no|yes','jazz|Fri Sat|line array|yes|lively',
    'yes|small plates|house highball|yes|23:30','no|yes|venue.example/book|24h|yes',
    '42|15|0|USD|18%','Central|A C|4|yes|yes','yes|36|no|12|4',
    'live set|weekly|online door|120 min|320','intimate|casual|low|25-40|moderate',
    'yes|no|outside|awning|yes','daytime|all ages|yes|no|yes','4|events only|yes|6|yes',
    'yes|yes|yes|yes|none','+1-555-0142|hello@venue.example|venue.example|@venue|events@venue.example',
    'Music District|42 Blue Note Ave|94107|37.7749|-122.4194','yes|60%|yes|yes|silver',
    'patio|yes|patio|monthly|yes','9m|6m|2|yes|rear dock','full|105|digital|24|48-channel',
    'mixed|34|28|10|yes','6|yes|yes|yes|events','yes|500 Mbps|good|yes|dedicated',
    'yes|yes|yes|digital|yes','yes|no flash|no|48h|front desk','QR|yes|evening|8 min|live',
    '17:00-19:00|10%|Tue|yes|newsletter','Central Market|high|busy|arts|7',
    'yes|move indoors|summer extended|yes|yes','contemporary|yes|yes|120 sqm|8',
    'weekly|arts fund|yes|70%|4.8','yes|3|4K|in-house|yes','LED|4K|multitrack|yes|200A',
    '480|60|900|4|350 meals','English Spanish|yes|on request|3|5',
    'active|450|passed|A|2026-06-14','yes|yes|no|yes|yes',
    '1998|The Workshop|M. Rivera|2024|local'
  ].map(values=>values.split('|'));
  function candidates(prune,readAnyway=null){
    return schemas.map((g,i)=>({...g,index:i,eligible:g.kind==='person',read:!prune||g.kind==='person'||i===readAnyway}));
  }
  function orders(choice){
    const visits=['VISITS','RECOMMENDS','FOLLOWS'],follows=['FOLLOWS','RECOMMENDS','VISITS'];
    return choice==='gem'?[visits,follows]:choice==='follows'?[follows,follows]:[visits,visits];
  }
  function profile(record,nid=67000){
    const category=(nid+record*13)%40;
    const section=profileSections[category];
    return {category,section:section.name,slots:200,entries:section.fields.map((key,j)=>({
      key,value:profileExamples[category][j],column:category*5+j,offset:j
    }))};
  }
  function initial(mode){return {mode,progress:1,prune:true,readAnyway:null,selected:null,
    view:'traversal',order:'gem',search:'gem',rewrites:false};}
  // Each entry is a presentation keyframe, not a live optimizer event.
  const tours={
    si:[{at:0,prune:false,progress:0},{at:950,prune:false,progress:.5},
      {at:1800,prune:false,progress:1},{at:2500,prune:true,progress:.4},{at:3400,prune:true,progress:1}],
    gem:[{at:0,view:'search',search:'shared',rewrites:false},{at:1100,search:'pushdown'},
      {at:2200,rewrites:true},{at:3300,search:'gem'},{at:4400,view:'traversal',order:'visits',progress:1},
      {at:5700,order:'gem',progress:1}]
  };
  const api={schemas,profileSections,candidates,orders,profile,initial,tours};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.TurboFeatureModel=api;
})(typeof window!=='undefined'?window:this);
