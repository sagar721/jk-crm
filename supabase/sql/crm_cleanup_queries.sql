begin;

delete from public.orders
where company_id in (
  select id from public.companies where name ilike 'QA Company %'
);

delete from public.quotations
where company_id in (
  select id from public.companies where name ilike 'QA Company %'
);

delete from public.inquiries
where company_id in (
  select id from public.companies where name ilike 'QA Company %'
);

delete from public.contacts
where email ilike '%@example.test'
   or name ilike 'Test %';

delete from public.companies
where name ilike 'QA Company %'
   or website ilike '%example.test%';

-- Orphan cleanup
delete from public.order_items oi
where not exists (
  select 1 from public.orders o where o.id = oi.order_id and o.user_id = oi.user_id
);

delete from public.quote_items qi
where not exists (
  select 1 from public.quotations q where q.id = qi.quotation_id and q.user_id = qi.user_id
);

delete from public.inquiry_items ii
where not exists (
  select 1 from public.inquiries i where i.id = ii.inquiry_id and i.user_id = ii.user_id
);

delete from public.orders o
where not exists (
  select 1 from public.companies c where c.id = o.company_id and c.user_id = o.user_id
)
   or (o.quotation_id is not null and not exists (
     select 1 from public.quotations q where q.id = o.quotation_id and q.user_id = o.user_id
   ));

delete from public.quotations q
where not exists (
  select 1 from public.companies c where c.id = q.company_id and c.user_id = q.user_id
)
   or (q.inquiry_id is not null and not exists (
     select 1 from public.inquiries i where i.id = q.inquiry_id and i.user_id = q.user_id
   ));

delete from public.inquiries i
where not exists (
  select 1 from public.companies c where c.id = i.company_id and c.user_id = i.user_id
)
   or (i.contact_id is not null and not exists (
     select 1 from public.contacts p where p.id = i.contact_id and p.user_id = i.user_id
   ));

delete from public.contacts p
where not exists (
  select 1 from public.companies c where c.id = p.company_id and c.user_id = p.user_id
);

commit;

-- Optional demo-seed cleanup:
-- delete from public.orders where company_id in (
--   select id from public.companies where name in ('Aarti Industries', 'Zydus Lifesciences', 'Torrent Power', 'Nirma Chemicals')
-- );
-- delete from public.quotations where company_id in (
--   select id from public.companies where name in ('Aarti Industries', 'Zydus Lifesciences', 'Torrent Power', 'Nirma Chemicals')
-- );
-- delete from public.inquiries where company_id in (
--   select id from public.companies where name in ('Aarti Industries', 'Zydus Lifesciences', 'Torrent Power', 'Nirma Chemicals')
-- );
-- delete from public.contacts where email ilike '%@aarti.example'
--    or email ilike '%@zydus.example'
--    or email ilike '%@torrent.example'
--    or email ilike '%@nirma.example';
-- delete from public.companies where name in ('Aarti Industries', 'Zydus Lifesciences', 'Torrent Power', 'Nirma Chemicals');
