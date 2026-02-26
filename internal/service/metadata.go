package service

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	registryv1connect "github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/schema"
)

type MetadataService struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

func NewMetadataService(pool *pgxpool.Pool, cache *schema.Cache) *MetadataService {
	return &MetadataService{pool: pool, cache: cache}
}

func (s *MetadataService) RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler) {
	return registryv1connect.NewMetadataServiceHandler(s, connect.WithInterceptors(interceptors...))
}

// ── Objects ─────────────────────────────────────────────────────────

func (s *MetadataService) ListObjects(ctx context.Context, req *connect.Request[registryv1.ListObjectsRequest]) (*connect.Response[registryv1.ListObjectsResponse], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, api_name, title, plural_title, COALESCE(description,''),
		       is_standard, COALESCE(storage_schema,''), COALESCE(storage_table,''),
		       supports_custom_fields, COALESCE(category_id::text,''),
		       created_at::text, updated_at::text
		FROM metadata.objects ORDER BY created_at
	`)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query objects: %w", err))
	}
	defer rows.Close()

	var objects []*registryv1.ObjectMeta
	for rows.Next() {
		o := &registryv1.ObjectMeta{}
		if err := rows.Scan(
			&o.Id, &o.ApiName, &o.Title, &o.PluralTitle, &o.Description,
			&o.IsStandard, &o.StorageSchema, &o.StorageTable,
			&o.SupportsCustomFields, &o.CategoryId,
			&o.CreatedAt, &o.UpdatedAt,
		); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("scan object: %w", err))
		}
		objects = append(objects, o)
	}
	if err := rows.Err(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&registryv1.ListObjectsResponse{Objects: objects}), nil
}

func (s *MetadataService) GetObject(ctx context.Context, req *connect.Request[registryv1.GetObjectRequest]) (*connect.Response[registryv1.GetObjectResponse], error) {
	o := &registryv1.ObjectMeta{}
	err := s.pool.QueryRow(ctx, `
		SELECT id, api_name, title, plural_title, COALESCE(description,''),
		       is_standard, COALESCE(storage_schema,''), COALESCE(storage_table,''),
		       supports_custom_fields, COALESCE(category_id::text,''),
		       created_at::text, updated_at::text
		FROM metadata.objects WHERE id = $1
	`, req.Msg.Id).Scan(
		&o.Id, &o.ApiName, &o.Title, &o.PluralTitle, &o.Description,
		&o.IsStandard, &o.StorageSchema, &o.StorageTable,
		&o.SupportsCustomFields, &o.CategoryId,
		&o.CreatedAt, &o.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query object: %w", err))
	}

	// Load fields for this object
	fields, err := s.listFieldsForObject(ctx, o.Id)
	if err != nil {
		return nil, err
	}
	o.Fields = fields

	return connect.NewResponse(&registryv1.GetObjectResponse{Object: o}), nil
}

func (s *MetadataService) CreateObject(ctx context.Context, req *connect.Request[registryv1.CreateObjectRequest]) (*connect.Response[registryv1.CreateObjectResponse], error) {
	msg := req.Msg
	o := &registryv1.ObjectMeta{}

	var categoryID *string
	if msg.CategoryId != "" {
		categoryID = &msg.CategoryId
	}

	err := s.pool.QueryRow(ctx, `
		INSERT INTO metadata.objects (api_name, title, plural_title, description, category_id, supports_custom_fields)
		VALUES ($1, $2, $3, NULLIF($4,''), $5::uuid, $6)
		RETURNING id, api_name, title, plural_title, COALESCE(description,''),
		          is_standard, COALESCE(storage_schema,''), COALESCE(storage_table,''),
		          supports_custom_fields, COALESCE(category_id::text,''),
		          created_at::text, updated_at::text
	`, msg.ApiName, msg.Title, msg.PluralTitle, msg.Description, categoryID, msg.SupportsCustomFields).Scan(
		&o.Id, &o.ApiName, &o.Title, &o.PluralTitle, &o.Description,
		&o.IsStandard, &o.StorageSchema, &o.StorageTable,
		&o.SupportsCustomFields, &o.CategoryId,
		&o.CreatedAt, &o.UpdatedAt,
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("create object: %w", err))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.CreateObjectResponse{Object: o}), nil
}

func (s *MetadataService) UpdateObject(ctx context.Context, req *connect.Request[registryv1.UpdateObjectRequest]) (*connect.Response[registryv1.UpdateObjectResponse], error) {
	msg := req.Msg
	o := &registryv1.ObjectMeta{}

	var categoryID *string
	if msg.CategoryId != "" {
		categoryID = &msg.CategoryId
	}

	err := s.pool.QueryRow(ctx, `
		UPDATE metadata.objects
		SET title = COALESCE(NULLIF($2,''), title),
		    plural_title = COALESCE(NULLIF($3,''), plural_title),
		    description = CASE WHEN $4 = '' THEN description ELSE $4 END,
		    category_id = COALESCE($5::uuid, category_id),
		    supports_custom_fields = $6,
		    updated_at = now()
		WHERE id = $1
		RETURNING id, api_name, title, plural_title, COALESCE(description,''),
		          is_standard, COALESCE(storage_schema,''), COALESCE(storage_table,''),
		          supports_custom_fields, COALESCE(category_id::text,''),
		          created_at::text, updated_at::text
	`, msg.Id, msg.Title, msg.PluralTitle, msg.Description, categoryID, msg.SupportsCustomFields).Scan(
		&o.Id, &o.ApiName, &o.Title, &o.PluralTitle, &o.Description,
		&o.IsStandard, &o.StorageSchema, &o.StorageTable,
		&o.SupportsCustomFields, &o.CategoryId,
		&o.CreatedAt, &o.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update object: %w", err))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.UpdateObjectResponse{Object: o}), nil
}

func (s *MetadataService) DeleteObject(ctx context.Context, req *connect.Request[registryv1.DeleteObjectRequest]) (*connect.Response[registryv1.DeleteObjectResponse], error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM metadata.objects WHERE id = $1`, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("delete object: %w", err))
	}
	if tag.RowsAffected() == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found"))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.DeleteObjectResponse{}), nil
}

// ── Fields ──────────────────────────────────────────────────────────

func (s *MetadataService) ListFields(ctx context.Context, req *connect.Request[registryv1.ListFieldsRequest]) (*connect.Response[registryv1.ListFieldsResponse], error) {
	fields, err := s.listFieldsForObject(ctx, req.Msg.ObjectId)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&registryv1.ListFieldsResponse{Fields: fields}), nil
}

func (s *MetadataService) GetField(ctx context.Context, req *connect.Request[registryv1.GetFieldRequest]) (*connect.Response[registryv1.GetFieldResponse], error) {
	f := &registryv1.FieldMeta{}
	err := s.pool.QueryRow(ctx, `
		SELECT id, object_id::text, api_name, title, COALESCE(description,''),
		       type, COALESCE(type_config::text,'{}'),
		       is_required, is_unique, is_standard,
		       COALESCE(storage_column,''), COALESCE(lookup_object_id::text,''),
		       created_at::text, updated_at::text
		FROM metadata.fields WHERE object_id = $1 AND id = $2
	`, req.Msg.ObjectId, req.Msg.Id).Scan(
		&f.Id, &f.ObjectId, &f.ApiName, &f.Title, &f.Description,
		&f.Type, &f.TypeConfig,
		&f.IsRequired, &f.IsUnique, &f.IsStandard,
		&f.StorageColumn, &f.LookupObjectId,
		&f.CreatedAt, &f.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("field not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query field: %w", err))
	}

	return connect.NewResponse(&registryv1.GetFieldResponse{Field: f}), nil
}

func (s *MetadataService) CreateField(ctx context.Context, req *connect.Request[registryv1.CreateFieldRequest]) (*connect.Response[registryv1.CreateFieldResponse], error) {
	msg := req.Msg
	f := &registryv1.FieldMeta{}

	var lookupObjID *string
	if msg.LookupObjectId != "" {
		lookupObjID = &msg.LookupObjectId
	}

	typeConfig := msg.TypeConfig
	if typeConfig == "" {
		typeConfig = "{}"
	}

	err := s.pool.QueryRow(ctx, `
		INSERT INTO metadata.fields (
			object_id, api_name, title, description, type, type_config,
			is_required, is_unique, lookup_object_id
		) VALUES ($1, $2, $3, NULLIF($4,''), $5, $6::jsonb, $7, $8, $9::uuid)
		RETURNING id, object_id::text, api_name, title, COALESCE(description,''),
		          type, COALESCE(type_config::text,'{}'),
		          is_required, is_unique, is_standard,
		          COALESCE(storage_column,''), COALESCE(lookup_object_id::text,''),
		          created_at::text, updated_at::text
	`, msg.ObjectId, msg.ApiName, msg.Title, msg.Description, msg.Type, typeConfig,
		msg.IsRequired, msg.IsUnique, lookupObjID).Scan(
		&f.Id, &f.ObjectId, &f.ApiName, &f.Title, &f.Description,
		&f.Type, &f.TypeConfig,
		&f.IsRequired, &f.IsUnique, &f.IsStandard,
		&f.StorageColumn, &f.LookupObjectId,
		&f.CreatedAt, &f.UpdatedAt,
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("create field: %w", err))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.CreateFieldResponse{Field: f}), nil
}

func (s *MetadataService) UpdateField(ctx context.Context, req *connect.Request[registryv1.UpdateFieldRequest]) (*connect.Response[registryv1.UpdateFieldResponse], error) {
	msg := req.Msg
	f := &registryv1.FieldMeta{}

	typeConfig := msg.TypeConfig
	if typeConfig == "" {
		typeConfig = "{}"
	}

	err := s.pool.QueryRow(ctx, `
		UPDATE metadata.fields
		SET title = COALESCE(NULLIF($3,''), title),
		    description = CASE WHEN $4 = '' THEN description ELSE $4 END,
		    type_config = CASE WHEN $5 = '{}' THEN type_config ELSE $5::jsonb END,
		    is_required = $6,
		    is_unique = $7,
		    updated_at = now()
		WHERE object_id = $1 AND id = $2
		RETURNING id, object_id::text, api_name, title, COALESCE(description,''),
		          type, COALESCE(type_config::text,'{}'),
		          is_required, is_unique, is_standard,
		          COALESCE(storage_column,''), COALESCE(lookup_object_id::text,''),
		          created_at::text, updated_at::text
	`, msg.ObjectId, msg.Id, msg.Title, msg.Description, typeConfig,
		msg.IsRequired, msg.IsUnique).Scan(
		&f.Id, &f.ObjectId, &f.ApiName, &f.Title, &f.Description,
		&f.Type, &f.TypeConfig,
		&f.IsRequired, &f.IsUnique, &f.IsStandard,
		&f.StorageColumn, &f.LookupObjectId,
		&f.CreatedAt, &f.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("field not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update field: %w", err))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.UpdateFieldResponse{Field: f}), nil
}

func (s *MetadataService) DeleteField(ctx context.Context, req *connect.Request[registryv1.DeleteFieldRequest]) (*connect.Response[registryv1.DeleteFieldResponse], error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM metadata.fields WHERE object_id = $1 AND id = $2`, req.Msg.ObjectId, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("delete field: %w", err))
	}
	if tag.RowsAffected() == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("field not found"))
	}

	s.reloadCache(ctx)
	return connect.NewResponse(&registryv1.DeleteFieldResponse{}), nil
}

// ── Helpers ─────────────────────────────────────────────────────────

func (s *MetadataService) listFieldsForObject(ctx context.Context, objectID string) ([]*registryv1.FieldMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, object_id::text, api_name, title, COALESCE(description,''),
		       type, COALESCE(type_config::text,'{}'),
		       is_required, is_unique, is_standard,
		       COALESCE(storage_column,''), COALESCE(lookup_object_id::text,''),
		       created_at::text, updated_at::text
		FROM metadata.fields WHERE object_id = $1 ORDER BY created_at
	`, objectID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query fields: %w", err))
	}
	defer rows.Close()

	var fields []*registryv1.FieldMeta
	for rows.Next() {
		f := &registryv1.FieldMeta{}
		if err := rows.Scan(
			&f.Id, &f.ObjectId, &f.ApiName, &f.Title, &f.Description,
			&f.Type, &f.TypeConfig,
			&f.IsRequired, &f.IsUnique, &f.IsStandard,
			&f.StorageColumn, &f.LookupObjectId,
			&f.CreatedAt, &f.UpdatedAt,
		); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("scan field: %w", err))
		}
		fields = append(fields, f)
	}
	if err := rows.Err(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return fields, nil
}

func (s *MetadataService) reloadCache(ctx context.Context) {
	// Best-effort reload; errors are logged but don't fail the mutation.
	_ = s.cache.Load(ctx, s.pool)
}
